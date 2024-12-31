using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using CommunityToolkit.HighPerformance;
using FastRail.Jutes;
using FastRail.Jutes.Proto;
using FastRail.Protos;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using RaftNET;
using RaftNET.Services;
using RaftNET.StateMachines;

namespace FastRail.Server;

public class RailServer : IDisposable, IStateMachine {
    private RaftServer? _raft;
    private readonly ILogger<RailServer> _logger;
    private readonly TcpListener _listener;
    private readonly CancellationTokenSource _cts = new();
    private readonly DataStore _ds;
    private const long SuperSecret = 0XB3415C00L;

    private readonly SessionTracker _sessionTracker;

    private readonly Config _config;
    private readonly ILoggerFactory _loggerFactory;

    public RailServer(
        Config config,
        ILoggerFactory loggerFactory
    ) {
        _config = config;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<RailServer>();
        _listener = new TcpListener(config.EndPoint);
        _ds = new DataStore(config.DataDir);
        _sessionTracker = new SessionTracker(
            TimeSpan.FromMilliseconds(config.Tick),
            sessionId => { _ds.RemoveSession(sessionId); },
            loggerFactory.CreateLogger<SessionTracker>());
    }

    public record Config {
        public required string DataDir;
        public required IPEndPoint EndPoint;
        public readonly TimeSpan ReceiveTimeout = TimeSpan.FromSeconds(6);
        public readonly int MinSessionTimeout = 1000;
        public readonly int MaxSessionTimeout = 10 * 1000;
        public readonly int Tick = 1000;
    }

    public void Start() {
        _ds.Load();
        _listener.Start();
        Task.Run(async () => {
            _logger.LogInformation("Rail server started at {}", _listener.LocalEndpoint);

            while (!_cts.Token.IsCancellationRequested) {
                var conn = await _listener.AcceptTcpClientAsync();
                _ = Task.Run(() => HandleConnection(conn, _cts.Token));
            }
            _logger.LogInformation("Rail server exited");
        }, _cts.Token);
    }

    public void Stop() {
        _cts.Cancel();
        _listener.Stop();
    }

    public void Dispose() {
        _cts.Dispose();
        _listener.Dispose();
        _sessionTracker.Dispose();
        _loggerFactory.Dispose();
    }

    private async Task Broadcast(Transaction transaction) {
        if (_raft == null) {
            _logger.LogWarning("Raft server is not running, can't broadcast TXNs");
            return;
        }
        var buffer = transaction.ToByteArray();
        await Task.Run(() => { _raft.AddEntryApplied(buffer); });
    }

    private async Task HandleConnection(TcpClient client, CancellationToken token) {
        // by now, only leader can handle connections
        _logger.LogInformation("Incoming client connection");
        await using var conn = client.GetStream();

        var connectRequest = await ReceiveConnectRequest(conn, token);
        _logger.LogInformation("Incoming connection request, timeout={} session_id={}",
            connectRequest.Timeout, connectRequest.SessionId);

        var connectResponse = CreateSession(connectRequest);
        var sessionId = connectResponse.SessionId;
        await Broadcast(new Transaction {
            CreateSession = new CreateSession {
                SessionId = sessionId,
                Password = ByteString.CopyFrom(connectResponse.Passwd),
                Timeout = connectResponse.Timeout,
                ReadOnly = false
            }
        });
        await SendConnectResponse(conn, token, connectResponse);
        _logger.LogInformation("New session created, id={} timeout={}", sessionId, connectResponse.Timeout);

        // we're good, handle client requests now
        var closing = false;

        while (!closing) {
            var (requestHeader, requestBuffer) = await ReceiveRequest(conn, token);
            var requestType = requestHeader.Type.ToEnum();
            var xid = requestHeader.Xid;
            _sessionTracker.Touch(sessionId);

            if (requestType == null) {
                throw new Exception($"Invalid request type: {requestHeader.Type}");
            }

            try {
                switch (requestType) {
                    case OpCode.Create: {
                        var request = JuteDeserializer.Deserialize<CreateRequest>(requestBuffer);

                        if (string.IsNullOrEmpty(request.Path) || !request.Flags.IsValidCreateMode()) {
                            throw new RailException(ErrorCodes.BadArguments);
                        }
                        var mode = request.Flags.ParseCreateMode();
                        var txn = new CreateNodeTransaction {
                            Path = request.Path,
                            Data = ByteString.CopyFrom(request.Data),
                            Mode = mode,
                            Ctime = Time.CurrentTimeMillis()
                        };

                        if (mode is CreateMode.Ephemeral or CreateMode.EphemeralSequential) {
                            txn.EphemeralOwner = sessionId;
                        }
                        await Broadcast(new Transaction { CreateNode = txn });
                        await SendResponse(conn, token, new ReplyHeader(xid, _ds.LastZxid),
                            new CreateResponse { Path = request.Path });
                        break;
                    }

                    case OpCode.Delete: {
                        var request = JuteDeserializer.Deserialize<DeleteRequest>(requestBuffer);

                        if (string.IsNullOrEmpty(request.Path)) {
                            throw new RailException(ErrorCodes.BadArguments);
                        }
                        await Broadcast(new Transaction { DeleteNode = new DeleteNodeTransaction { Path = request.Path } });
                        await SendResponse(conn, token, new ReplyHeader(xid, _ds.LastZxid));
                        break;
                    }

                    case OpCode.Exists: {
                        var request = JuteDeserializer.Deserialize<ExistsRequest>(requestBuffer);

                        if (string.IsNullOrEmpty(request.Path)) {
                            throw new RailException(ErrorCodes.BadArguments);
                        }
                        var node = _ds.GetNode(request.Path);

                        if (node == null) {
                            throw new RailException(ErrorCodes.NoNode);
                        }
                        var children = _ds.CountNodeChildren(request.Path);
                        var stat = node.Stat.ToStat(node.Data.Length, children);
                        await SendResponse(conn,
                            token, new ReplyHeader(xid, _ds.LastZxid), new ExistsResponse { Stat = stat });
                        break;
                    }

                    case OpCode.GetData: {
                        var request = JuteDeserializer.Deserialize<GetDataRequest>(requestBuffer);

                        if (string.IsNullOrEmpty(request.Path)) {
                            throw new RailException(ErrorCodes.BadArguments);
                        }
                        var node = _ds.GetNode(request.Path);

                        if (node == null) {
                            throw new RailException(ErrorCodes.NoNode);
                        }

                        var children = _ds.CountNodeChildren(request.Path);
                        var stat = node.Stat.ToStat(node.Data.Length, children);
                        await SendResponse(conn, token,
                            new ReplyHeader(xid, _ds.LastZxid),
                            new GetDataResponse { Data = node.Data, Stat = stat });
                        break;
                    }

                    case OpCode.SetData: {
                        var request = JuteDeserializer.Deserialize<SetDataRequest>(requestBuffer);

                        if (string.IsNullOrEmpty(request.Path)) {
                            throw new RailException(ErrorCodes.BadArguments);
                        }

                        var node = _ds.GetNode(request.Path);

                        if (node == null) {
                            throw new RailException(ErrorCodes.NoNode);
                        }

                        var txn = new UpdateNodeTransaction {
                            Path = request.Path,
                            Mtime = Time.CurrentTimeMillis(),
                            Version = node.Stat.Version + 1
                        };

                        if (request.Data != null) {
                            txn.Data = ByteString.CopyFrom(request.Data);
                        }
                        await Broadcast(new Transaction { UpdateNode = txn });
                        break;
                    }

                    case OpCode.GetACL:
                    case OpCode.SetACL: {
                        throw new NotImplementedException();
                    }

                    case OpCode.GetChildren:
                    case OpCode.GetChildren2: {
                        var request = JuteDeserializer.Deserialize<GetChildrenRequest>(requestBuffer);

                        if (string.IsNullOrEmpty(request.Path)) {
                            throw new RailException(ErrorCodes.BadArguments);
                        }
                        var children = _ds.GetChildren(request.Path, out var stat);

                        if (requestType == OpCode.GetChildren) {
                            await SendResponse(conn, token,
                                new ReplyHeader(xid, _ds.LastZxid),
                                new GetChildrenResponse { Children = children });
                        } else {
                            await SendResponse(conn, token,
                                new ReplyHeader(xid, _ds.LastZxid),
                                new GetChildren2Response { Children = children, Stat = stat.ToStat() }
                            );
                        }

                        break;
                    }

                    case OpCode.Sync: {
                        var request = JuteDeserializer.Deserialize<SyncRequest>(requestBuffer);

                        if (string.IsNullOrEmpty(request.Path)) {
                            throw new RailException(ErrorCodes.BadArguments);
                        }
                        await Broadcast(new Transaction { Sync = new SyncTransaction { Path = request.Path } });
                        break;
                    }

                    case OpCode.Ping: {
                        _logger.LogInformation("Client ping");
                        var pingXid = -2;
                        var lastZxid = _ds.LastZxid;
                        await SendResponse(conn, token, new ReplyHeader(pingXid, lastZxid));
                        break;
                    }

                    case OpCode.Check:
                        break;
                    case OpCode.Multi:
                        break;
                    case OpCode.Create2:
                        break;
                    case OpCode.Reconfig:
                        break;
                    case OpCode.CheckWatches:
                        break;
                    case OpCode.RemoveWatches:
                        break;
                    case OpCode.CreateContainer:
                        break;
                    case OpCode.DeleteContainer:
                        break;
                    case OpCode.CreateTtl:
                        break;
                    case OpCode.MultiRead:
                        break;
                    case OpCode.Auth:
                        break;
                    case OpCode.SetWatches:
                        break;
                    case OpCode.Sasl:
                        break;
                    case OpCode.GetEphemerals:
                        break;
                    case OpCode.GetAllChildrenNumber:
                        break;
                    case OpCode.SetWatches2:
                        break;
                    case OpCode.AddWatch:
                        break;
                    case OpCode.WhoAmI:
                        break;
                    case OpCode.CreateSession: {
                        break;
                    }
                    case OpCode.CloseSession: {
                        _sessionTracker.Remove(sessionId);
                        closing = true;
                        break;
                    }
                    case OpCode.Error:
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            } catch (RailException ex) {
                var err = ex.Err;
                await SendResponse(conn, token, new ReplyHeader(xid, _ds.LastZxid, err));
            }
        }
        _logger.LogInformation("Client connection closed");
        await Task.CompletedTask;
    }

    private ConnectResponse CreateSession(ConnectRequest request) {
        _logger.LogInformation("Client attempting to establish new session");
        var sessionTimeout = int.Min(int.Max(request.Timeout, _config.MinSessionTimeout), _config.MaxSessionTimeout);
        var sessionId = _sessionTracker.Add(TimeSpan.FromMilliseconds(sessionTimeout));
        var passwd = request.Passwd ?? [];
        var rnd = new Random((int)(sessionId ^ SuperSecret));
        rnd.NextBytes(passwd);
        return new ConnectResponse {
            ProtocolVersion = 0,
            SessionId = sessionId,
            Passwd = passwd,
            Timeout = sessionTimeout,
            ReadOnly = false // not supported
        };
    }

    private async Task<(RequestHeader, byte[])> ReceiveRequest(NetworkStream stream, CancellationToken token) {
        // len
        var lengthBuffer = new byte[sizeof(int)];
        await stream.ReadExactlyAsync(lengthBuffer, 0, lengthBuffer.Length, token);
        var packetLength = BinaryPrimitives.ReadInt32BigEndian(lengthBuffer);

        // request header
        var headerBuffer = new byte[RequestHeader.SizeOf];
        await stream.ReadExactlyAsync(headerBuffer, 0, headerBuffer.Length, token);
        var header = JuteDeserializer.Deserialize<RequestHeader>(headerBuffer);

        // body
        var bodyBuffer = new byte[packetLength - RequestHeader.SizeOf];
        await stream.ReadExactlyAsync(bodyBuffer, 0, bodyBuffer.Length, token);
        _logger.LogTrace("Received request, xid={} op={} len={}", header.Xid, header.Type, bodyBuffer.Length);
        return await Task.FromResult((header, bodyBuffer));
    }

    private async Task<ConnectRequest> ReceiveConnectRequest(NetworkStream stream, CancellationToken token) {
        // len
        var lengthBuffer = new byte[sizeof(int)];
        await stream.ReadExactlyAsync(lengthBuffer, 0, lengthBuffer.Length, token);
        var packetLength = BinaryPrimitives.ReadInt32BigEndian(lengthBuffer);
        // connect request body
        var bodyBuffer = new byte[packetLength];
        await stream.ReadExactlyAsync(bodyBuffer, 0, bodyBuffer.Length, token);
        return JuteDeserializer.Deserialize<ConnectRequest>(bodyBuffer);
    }

    private async Task SendConnectResponse(NetworkStream stream, CancellationToken token, ConnectResponse response) {
        var buffer = JuteSerializer.Serialize(response);
        var lengthBuffer = new byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(lengthBuffer, buffer.Length);
        // len
        await stream.WriteAsync(lengthBuffer, 0, lengthBuffer.Length, token);
        // connect response
        await stream.WriteAsync(buffer, 0, buffer.Length, token);
    }

    private async Task SendResponse<T>(NetworkStream stream, CancellationToken token, ReplyHeader header, T response)
        where T : IJuteSerializable {
        var headerBuffer = JuteSerializer.Serialize(header);
        var bodyBuffer = JuteSerializer.Serialize(response);
        var len = headerBuffer.Length + bodyBuffer.Length;
        var lenBuffer = new byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(lenBuffer, len);
        await stream.WriteAsync(lenBuffer, 0, lenBuffer.Length, token);
        await stream.WriteAsync(headerBuffer, 0, headerBuffer.Length, token);
        await stream.WriteAsync(bodyBuffer, 0, bodyBuffer.Length, token);
    }

    private async Task SendResponse(NetworkStream stream, CancellationToken token, ReplyHeader header) {
        var headerBuffer = JuteSerializer.Serialize(header);
        var len = headerBuffer.Length;
        var lenBuffer = new byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(lenBuffer, len);
        _logger.LogInformation("Sending response");
        await stream.WriteAsync(lenBuffer, 0, lenBuffer.Length, token);
        await stream.WriteAsync(headerBuffer, 0, headerBuffer.Length, token);
        _logger.LogInformation("Send response len={}", lenBuffer.Length);
    }

    public void Apply(List<Command> commands) {
        foreach (var command in commands) {
            var transaction = Transaction.Parser.ParseFrom(command.Buffer);

            switch (transaction.TxnCase) {
                case Transaction.TxnOneofCase.CreateNode: {
                    var txn = transaction.CreateNode;
                    _ds.CreateNode(txn.Path, txn.Data.ToByteArray(), new StatEntry {
                        Czxid = txn.Zxid,
                        Mzxid = 0,
                        Ctime = txn.Ctime,
                        Mtime = 0,
                        Version = 0,
                        Cversion = 0,
                        Aversion = 0,
                        EphemeralOwner = txn.EphemeralOwner,
                        Pzxid = 0
                    });
                    break;
                }
                case Transaction.TxnOneofCase.DeleteNode: {
                    var txn = transaction.DeleteNode;
                    _ds.RemoveNode(txn.Path);
                    break;
                }
                case Transaction.TxnOneofCase.UpdateNode: {
                    var txn = transaction.UpdateNode;
                    break;
                }
                case Transaction.TxnOneofCase.Sync: {
                    // do nothing, we're already synced
                    break;
                }
                case Transaction.TxnOneofCase.CreateSession: {
                    var txn = transaction.CreateSession;
                    break;
                }
                case Transaction.TxnOneofCase.RemoveSession: {
                    var txn = transaction.RemoveSession;
                    _ds.RemoveSession(txn.SessionId);
                    break;
                }
                case Transaction.TxnOneofCase.None:
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }

    public ulong TakeSnapshot() {
        throw new NotImplementedException();
    }

    public void DropSnapshot(ulong snapshot) {
        throw new NotImplementedException();
    }

    public void LoadSnapshot(ulong snapshot) {
        throw new NotImplementedException();
    }

    public void OnEvent(Event ev) {
        ev.Switch(e => {
            _logger.LogInformation("Role changed, id={} role={} ", e.ServerId, e.Role);
        });
    }

    public void SetRaftServer(RaftServer raftServer) {
        _raft = raftServer;
    }
}