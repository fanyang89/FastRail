using System.Buffers.Binary;
using System.ComponentModel;
using System.Net;
using System.Net.Sockets;
using FastRail.Exceptions;
using FastRail.Jutes;
using FastRail.Jutes.Proto;
using FastRail.Protos;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using RaftNET;
using RaftNET.Services;
using RaftNET.StateMachines;
using SystemException = FastRail.Exceptions.SystemException;
using WatcherEvent = FastRail.Jutes.Proto.WatcherEvent;

namespace FastRail.Server;

public class Server : IDisposable, IStateMachine {
    public record Config {
        public readonly TimeSpan ReceiveTimeout = TimeSpan.FromSeconds(6);
        public readonly int MinSessionTimeout = 1000;
        public readonly int MaxSessionTimeout = 10 * 1000;
        public readonly int Tick = 1000;
        public required string DataDir;
        public required IPEndPoint EndPoint;
    }

    private const long SuperSecret = 0XB3415C00L;
    private readonly ILogger<Server> _logger;
    private readonly TcpListener _listener;
    private readonly CancellationTokenSource _cts = new();
    private readonly DataStore _ds;
    private readonly Config _config;
    private readonly ILoggerFactory _loggerFactory;
    private readonly WatcherManager _watcherManager = new();

    public Server(
        Config config,
        ILoggerFactory loggerFactory
    ) {
        _config = config;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<Server>();
        _listener = new TcpListener(config.EndPoint);
        _ds = new DataStore(config.DataDir, _loggerFactory.CreateLogger<DataStore>());
    }

    public RaftServer? Raft { get; set; }
    public int PingCount { get; private set; }

    public void Dispose() {
        _cts.Dispose();
        _listener.Dispose();
        GC.SuppressFinalize(this);
    }

    public void Apply(List<Command> commands) {
        foreach (var transaction in commands
                     .Select(command => Transaction.Parser.ParseFrom(command.Buffer))) {
            var zxid = transaction.Zxid;
            switch (transaction.TxnCase) {
                case Transaction.TxnOneofCase.CreateNode: {
                    var txn = transaction.CreateNode;
                    _ds.CreateNode(zxid, txn);
                    break;
                }
                case Transaction.TxnOneofCase.DeleteNode: {
                    var txn = transaction.DeleteNode;
                    _ds.RemoveNode(zxid, txn);
                    break;
                }
                case Transaction.TxnOneofCase.UpdateNode: {
                    var txn = transaction.UpdateNode;
                    _ds.UpdateNode(zxid, txn);
                    break;
                }
                case Transaction.TxnOneofCase.Sync: {
                    // do nothing, we're already synced
                    break;
                }
                case Transaction.TxnOneofCase.CreateSession: {
                    var txn = transaction.CreateSession;
                    _ds.CreateSession(zxid, txn);
                    break;
                }
                case Transaction.TxnOneofCase.RemoveSession: {
                    var txn = transaction.RemoveSession;
                    _ds.RemoveSession(txn.SessionId);
                    break;
                }
                case Transaction.TxnOneofCase.None:
                default:
                    throw new InvalidDataException();
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
        ev.Switch(e => { _logger.LogInformation("Role changed, id={} role={} ", e.ServerId, e.Role); });
    }

    public void Start() {
        _ds.Start();
        _listener.Start();
        Task.Run(async () => {
            _logger.LogInformation("Rail server started at {}", _listener.LocalEndpoint);
            while (!_cts.Token.IsCancellationRequested) {
                var conn = await _listener.AcceptTcpClientAsync();
                _ = Task.Run(async () => {
                    try {
                        await HandleConnection(conn, _cts.Token);
                    }
                    catch (OperationCanceledException) {
                        // ignored
                    }
                    catch (Exception e) {
                        _logger.LogError(e, "Handle connection failed");
                    }
                });
            }
            _logger.LogInformation("Rail server exited");
        }, _cts.Token);
    }

    public void Stop() {
        _cts.Cancel();
        _listener.Stop();
        _ds.Stop();
    }

    private async Task Broadcast(Transaction transaction) {
        if (Raft == null) {
            _logger.LogWarning("Raft server is not running, can't broadcast TXNs");
            return;
        }
        transaction.Zxid = _ds.NextZxid;
        var buffer = transaction.ToByteArray();
        await Task.Run(() => { Raft.AddEntryApplied(buffer); });
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
            Zxid = _ds.NextZxid,
            CreateSession = new CreateSessionTransaction {
                Session = new SessionEntry {
                    Id = sessionId,
                    Password = ByteString.CopyFrom(connectResponse.Passwd),
                    Timeout = connectResponse.Timeout,
                    ReadOnly = false,
                    LastLive = Time.CurrentTimeMillis()
                }
            }
        });
        await SendConnectResponse(conn, connectResponse, token);
        _logger.LogInformation("New session created, id={} timeout={}", sessionId, connectResponse.Timeout);

        // we're good, handle client requests now
        var closing = false;
        while (!closing) {
            var (requestHeader, requestBuffer) = await ReceiveRequest(conn, token);
            var requestType = requestHeader.Type.ToEnum();
            var xid = requestHeader.Xid;

            if (requestType == null) {
                _logger.LogError("Invalid request, type is null");
                throw new RailException(ErrorCodes.BadArguments);
            }

            try {
                switch (requestType) {
                    case OpCode.Create:
                    case OpCode.Create2:
                    case OpCode.CreateContainer: {
                        var request = JuteDeserializer.Deserialize<CreateRequest>(requestBuffer);
                        await HandleCreateRequest(conn, request, sessionId, xid, requestType.Value, token);
                        break;
                    }

                    case OpCode.CreateTTL: {
                        var request = JuteDeserializer.Deserialize<CreateTtlRequest>(requestBuffer);
                        await HandleCreateTTLRequest(conn, request, xid, token);
                        break;
                    }

                    case OpCode.Delete:
                    case OpCode.DeleteContainer: {
                        var request = JuteDeserializer.Deserialize<DeleteRequest>(requestBuffer);
                        await HandleDeleteRequest(conn, request, xid, token);
                        break;
                    }

                    case OpCode.Exists: {
                        var request = JuteDeserializer.Deserialize<ExistsRequest>(requestBuffer);
                        await HandleExistsRequest(conn, request, xid, token);
                        break;
                    }

                    case OpCode.GetData: {
                        var request = JuteDeserializer.Deserialize<GetDataRequest>(requestBuffer);
                        await HandleGetDataRequest(conn, request, xid, token);
                        break;
                    }

                    case OpCode.SetData: {
                        var request = JuteDeserializer.Deserialize<SetDataRequest>(requestBuffer);
                        await HandleSetDataRequest(conn, request, xid, token);
                        break;
                    }

                    case OpCode.GetChildren: {
                        var request = JuteDeserializer.Deserialize<GetChildrenRequest>(requestBuffer);
                        await HandleGetChildrenRequest(conn, request, xid, token);
                        break;
                    }
                    case OpCode.GetChildren2: {
                        var request = JuteDeserializer.Deserialize<GetChildren2Request>(requestBuffer);
                        await HandleGetChildren2Request(conn, request, xid, token);
                        break;
                    }

                    case OpCode.Sync: {
                        var request = JuteDeserializer.Deserialize<SyncRequest>(requestBuffer);
                        await HandleSyncRequest(conn, request, xid, token);
                        break;
                    }

                    case OpCode.Ping: {
                        PingCount++;
                        _ds.TouchSession(sessionId);
                        await SendResponse(conn, new ReplyHeader(xid, _ds.LastZxid), token);
                        break;
                    }

                    case OpCode.Check: {
                        var request = JuteDeserializer.Deserialize<CheckVersionRequest>(requestBuffer);
                        await HandleCheckVersionRequest(conn, request, xid, token);
                        break;
                    }

                    case OpCode.GetEphemerals: {
                        var request = JuteDeserializer.Deserialize<GetEphemeralsRequest>(requestBuffer);
                        await HandleGetEphemeralsRequest(conn, request, xid, token);
                        break;
                    }

                    case OpCode.GetAllChildrenNumber: {
                        var request = JuteDeserializer.Deserialize<GetAllChildrenNumberRequest>(requestBuffer);
                        await HandleGetAllChildrenNumberRequest(conn, request, xid, token);
                        break;
                    }

                    case OpCode.CreateSession:
                        throw new NotImplementedException();

                    case OpCode.CloseSession: {
                        _ds.RemoveSession(sessionId);
                        closing = true;
                        break;
                    }

                    case OpCode.WhoAmI: {
                        await SendResponse(conn, new ReplyHeader(xid, _ds.LastZxid), new WhoAmIResponse { ClientInfo = null },
                            token);
                        throw new NotImplementedException();
                    }

                    case OpCode.AddWatch: {
                        var request = JuteDeserializer.Deserialize<AddWatchRequest>(requestBuffer);
                        HandleAddWatchRequest(conn, request, xid, token);
                        break;
                    }

                    case OpCode.SetWatches: {
                        var request = JuteDeserializer.Deserialize<SetWatches>(requestBuffer);
                        break;
                    }

                    case OpCode.SetWatches2: {
                        var request = JuteDeserializer.Deserialize<SetWatches2>(requestBuffer);
                        break;
                    }

                    case OpCode.CheckWatches: {
                        var request = JuteDeserializer.Deserialize<CheckWatchesRequest>(requestBuffer);
                        break;
                    }

                    case OpCode.RemoveWatches: {
                        var request = JuteDeserializer.Deserialize<RemoveWatchesRequest>(requestBuffer);
                        break;
                    }

                    case OpCode.Reconfig:

                    case OpCode.GetACL:
                    case OpCode.SetACL:

                    case OpCode.Multi:
                    case OpCode.MultiRead:
                    case OpCode.Error:

                    case OpCode.Auth:
                    case OpCode.SASL:
                        throw new NotImplementedException();

                    default:
                        throw new InvalidEnumArgumentException(nameof(requestType));
                }
            }
            catch (RailException ex) {
                var err = ex.Err;
                await SendResponse(conn, new ReplyHeader(xid, _ds.LastZxid, err), token);
            }
        }
        _logger.LogInformation("Client connection closed, sessionId={}", sessionId);
        client.Close();
    }

    private void HandleAddWatchRequest(NetworkStream conn, AddWatchRequest request, int xid, CancellationToken token) {
        if (string.IsNullOrWhiteSpace(request.Path)
            || request.Mode > (int)AddWatchMode.PersistentRecursive || request.Mode <= (int)AddWatchMode.Persistent) {
            throw new RailException(ErrorCodes.BadArguments);
        }

        var watchMode = (AddWatchMode)request.Mode;
        var watchPath = request.Path;
        switch (conn) {}
    }

    private async Task HandleCreateTTLRequest(NetworkStream conn, CreateTtlRequest request, int xid,
        CancellationToken token) {
        var createMode = request.Flags.ParseCreateMode();
        if (createMode != CreateMode.Ttl && createMode != CreateMode.PersistentSequentialWithTtl) {
            throw new RailException(ErrorCodes.BadArguments);
        }
        if (string.IsNullOrEmpty(request.Path) || !request.Flags.IsValidCreateMode()) {
            throw new RailException(ErrorCodes.BadArguments);
        }
        if (request.Ttl <= 0) {
            throw new RailException(ErrorCodes.BadArguments);
        }

        var txn = new CreateNodeTransaction {
            Path = request.Path,
            Data = ByteString.CopyFrom(request.Data),
            Ctime = Time.CurrentTimeMillis(),
            EphemeralOwner = 0,
            Ttl = request.Ttl,
            IsContainer = false,
            IsSequential = false
        };
        await Broadcast(new Transaction { Zxid = _ds.NextZxid, CreateNode = txn });
        await SendResponse(conn, new ReplyHeader(xid, _ds.LastZxid), new CreateResponse { Path = request.Path }, token);
        _logger.LogInformation("TTL node created, path={} ttl={}ms", request.Path, request.Ttl);
        _ = Task.Run(async () => {
            await Task.Delay(TimeSpan.FromMilliseconds(request.Ttl), token);
            _logger.LogInformation("Removing expired TTL node, path={} ttl={}ms", request.Path, request.Ttl);
            _ds.RemoveNode(_ds.NextZxid, new DeleteNodeTransaction { Path = null });
        }, token);
    }

    private async Task HandleGetAllChildrenNumberRequest(NetworkStream conn, GetAllChildrenNumberRequest request, int xid,
        CancellationToken token) {
        var count = request.Path == null ? _ds.CountAllChildren() : _ds.CountAllChildren(request.Path);
        await SendResponse(conn, new ReplyHeader(xid, _ds.LastZxid), new GetAllChildrenNumberResponse { TotalNumber = count },
            token);
    }

    private async Task HandleGetEphemeralsRequest(NetworkStream conn, GetEphemeralsRequest request, int xid,
        CancellationToken token) {
        var nodes = _ds.ListEphemeral(request.PrefixPath);
        await SendResponse(conn, new ReplyHeader(xid, _ds.LastZxid),
            new GetEphemeralsResponse { Ephemerals = nodes },
            token);
    }

    private Task HandleCheckVersionRequest(NetworkStream conn, CheckVersionRequest request, int xid,
        CancellationToken token) {
        if (request.Path == null) {
            throw new RailException(ErrorCodes.BadArguments);
        }
        throw new NotImplementedException();
    }

    private async Task HandleSetDataRequest(NetworkStream conn, SetDataRequest request, int xid, CancellationToken token) {
        if (string.IsNullOrEmpty(request.Path)) {
            throw new RailException(ErrorCodes.BadArguments);
        }

        var stat = _ds.GetNodeStat(request.Path);
        if (stat == null) {
            throw new RailException(ErrorCodes.NoNode);
        }
        if (request.Version != -1 && stat.Version != request.Version) {
            throw new RailException(ErrorCodes.BadVersion);
        }

        var txn = new UpdateNodeTransaction {
            Path = request.Path,
            Mtime = Time.CurrentTimeMillis(),
            Version = stat.Version + 1,
            Data = ByteString.CopyFrom(request.Data)
        };

        await Broadcast(new Transaction { Zxid = _ds.NextZxid, UpdateNode = txn });

        stat = _ds.GetNodeStat(request.Path);
        if (stat == null) {
            throw new SystemException("Get stat failed after node updated");
        }

        await SendResponse(conn, new ReplyHeader(xid, _ds.LastZxid), new SetDataResponse { Stat = stat.ToJuteStat() }, token);
    }

    private async Task HandleSyncRequest(NetworkStream conn, SyncRequest request, int xid, CancellationToken token) {
        if (string.IsNullOrEmpty(request.Path)) {
            throw new RailException(ErrorCodes.BadArguments);
        }
        await Broadcast(new Transaction { Sync = new SyncTransaction { Path = request.Path } });
        await SendResponse(conn, new ReplyHeader(xid, _ds.LastZxid), new SyncResponse { Path = request.Path }, token);
    }

    private async Task HandleGetChildren2Request(
        NetworkStream conn, GetChildren2Request request, int xid, CancellationToken token
    ) {
        if (string.IsNullOrEmpty(request.Path)) {
            throw new RailException(ErrorCodes.BadArguments);
        }
        var children = _ds.GetChildren(request.Path, out var stat);
        await SendResponse(conn,
            new ReplyHeader(xid, _ds.LastZxid),
            new GetChildren2Response { Children = children, Stat = stat.ToJuteStat() }, token);
    }

    private async Task HandleGetChildrenRequest(NetworkStream conn, GetChildrenRequest request,
        int xid, CancellationToken token) {
        if (string.IsNullOrEmpty(request.Path)) {
            throw new RailException(ErrorCodes.BadArguments);
        }
        var children = _ds.GetChildren(request.Path, out _);
        await SendResponse(conn,
            new ReplyHeader(xid, _ds.LastZxid),
            new GetChildrenResponse { Children = children }, token);
    }

    private async Task HandleGetDataRequest(NetworkStream conn, GetDataRequest request, int xid, CancellationToken token) {
        if (string.IsNullOrEmpty(request.Path)) {
            throw new RailException(ErrorCodes.BadArguments);
        }

        var stat = _ds.GetNodeStat(request.Path);
        var data = _ds.GetNodeData(request.Path);
        if (stat == null || data == null) {
            throw new RailException(ErrorCodes.NoNode);
        }

        await SendResponse(conn,
            new ReplyHeader(xid, _ds.LastZxid),
            new GetDataResponse { Data = data, Stat = stat.ToJuteStat() }, token);
    }

    private async Task HandleExistsRequest(NetworkStream conn, ExistsRequest request, int xid, CancellationToken token) {
        if (string.IsNullOrEmpty(request.Path)) {
            throw new RailException(ErrorCodes.BadArguments);
        }

        var response = new ExistsResponse { Stat = null };
        var stat = _ds.GetNodeStat(request.Path);
        if (stat != null) {
            response.Stat = stat.ToJuteStat();
        }
        await SendResponse(conn, new ReplyHeader(xid, _ds.LastZxid), response, token);
    }

    private async Task HandleDeleteRequest(NetworkStream conn, DeleteRequest request, int xid, CancellationToken token) {
        if (string.IsNullOrEmpty(request.Path)) {
            throw new RailException(ErrorCodes.BadArguments);
        }

        await Broadcast(new Transaction { DeleteNode = new DeleteNodeTransaction { Path = request.Path } });
        await SendResponse(conn, new ReplyHeader(xid, _ds.LastZxid), token);
    }

    private async Task HandleCreateRequest(
        NetworkStream conn, CreateRequest request, long sessionId, int xid, OpCode opCode, CancellationToken token
    ) {
        var createMode = request.Flags.ParseCreateMode();
        var isContainer = opCode == OpCode.CreateContainer;
        if (createMode == CreateMode.Container && !isContainer) {
            throw new RailException(ErrorCodes.BadArguments);
        }
        if (string.IsNullOrEmpty(request.Path) || !request.Flags.IsValidCreateMode()) {
            throw new RailException(ErrorCodes.BadArguments);
        }

        long? sequence = null;
        if (createMode is CreateMode.Sequence) {
            var lastPrefixPath = _ds.LastPathByPrefix(request.Path);
            var sequenceStr = lastPrefixPath.TrimPrefix(request.Path);
            sequence = long.Parse(sequenceStr) + 1;
        }

        string path;
        if (sequence != null) {
            path = request.Path + $"{sequence:08d}";
        } else {
            path = request.Path;
        }

        var txn = new CreateNodeTransaction {
            Path = path,
            Data = ByteString.CopyFrom(request.Data),
            Ctime = Time.CurrentTimeMillis(),
            IsContainer = isContainer,
            IsSequential = sequence != null,
            Ttl = 0
        };
        if (createMode is CreateMode.Ephemeral or CreateMode.EphemeralSequential) {
            txn.EphemeralOwner = sessionId;
        }

        await Broadcast(new Transaction { Zxid = _ds.NextZxid, CreateNode = txn });

        switch (opCode) {
            case OpCode.Create:
            case OpCode.CreateContainer:
                await SendResponse(conn, new ReplyHeader(xid, _ds.LastZxid),
                    new CreateResponse { Path = request.Path }, token);
                break;
            case OpCode.Create2: {
                var stat = _ds.GetNodeStat(request.Path);
                if (stat == null) {
                    _logger.LogError("Node created just now but not found");
                    throw new RailException(ErrorCodes.SystemError);
                }
                await SendResponse(conn, new ReplyHeader(xid, _ds.LastZxid),
                    new Create2Response { Path = request.Path, Stat = stat.ToJuteStat() }, token);
                break;
            }
            default:
                throw new InvalidEnumArgumentException(nameof(opCode));
        }
    }

    private ConnectResponse CreateSession(ConnectRequest request) {
        _logger.LogInformation("Client attempting to establish new session");
        var sessionTimeout = int.Min(int.Max(request.Timeout, _config.MinSessionTimeout), _config.MaxSessionTimeout);
        var sessionId = _ds.CreateSession(TimeSpan.FromMilliseconds(sessionTimeout));
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

    private static async Task<ConnectRequest> ReceiveConnectRequest(NetworkStream stream, CancellationToken token) {
        var lengthBuffer = new byte[sizeof(int)];
        await stream.ReadExactlyAsync(lengthBuffer, 0, lengthBuffer.Length, token);
        var packetLength = BinaryPrimitives.ReadInt32BigEndian(lengthBuffer);
        var bodyBuffer = new byte[packetLength];
        await stream.ReadExactlyAsync(bodyBuffer, 0, bodyBuffer.Length, token);
        return JuteDeserializer.Deserialize<ConnectRequest>(bodyBuffer);
    }

    private static async Task SendConnectResponse(NetworkStream stream, ConnectResponse response, CancellationToken token) {
        var buffer = JuteSerializer.Serialize(response);
        var lengthBuffer = new byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(lengthBuffer, buffer.Length);
        await stream.WriteAsync(lengthBuffer, token);
        await stream.WriteAsync(buffer, token);
    }

    private static async Task SendResponse<T>(NetworkStream stream, ReplyHeader header, T response, CancellationToken token)
        where T : IJuteSerializable {
        var headerBuffer = JuteSerializer.Serialize(header);
        var bodyBuffer = JuteSerializer.Serialize(response);
        var len = headerBuffer.Length + bodyBuffer.Length;
        var lenBuffer = new byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(lenBuffer, len);
        await stream.WriteAsync(lenBuffer, token);
        await stream.WriteAsync(headerBuffer, token);
        await stream.WriteAsync(bodyBuffer, token);
    }

    private static async Task SendResponse(NetworkStream stream, ReplyHeader header, CancellationToken token) {
        var headerBuffer = JuteSerializer.Serialize(header);
        var len = headerBuffer.Length;
        var lenBuffer = new byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(lenBuffer, len);
        await stream.WriteAsync(lenBuffer, token);
        await stream.WriteAsync(headerBuffer, token);
    }

    private static async Task SendWatchEvent(NetworkStream stream, long zxid, WatcherEvent e, CancellationToken token) {
        var headerBuffer = JuteSerializer.Serialize(new ReplyHeader(-1, zxid));
        var bodyBuffer = JuteSerializer.Serialize(e);
        var len = headerBuffer.Length + bodyBuffer.Length;
        var lenBuffer = new byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(lenBuffer, len);
        await stream.WriteAsync(lenBuffer, token);
        await stream.WriteAsync(headerBuffer, token);
        await stream.WriteAsync(bodyBuffer, token);
    }
}