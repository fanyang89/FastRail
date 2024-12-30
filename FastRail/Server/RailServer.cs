using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using FastRail.Jutes;
using FastRail.Jutes.Data;
using FastRail.Jutes.Proto;
using Microsoft.Extensions.Logging;
using RaftNET;
using RaftNET.Services;
using RaftNET.StateMachines;

namespace FastRail.Server;

public class RailServer(
    RailServer.Config config,
    ILoggerFactory loggerFactory
) : IDisposable, IStateMachine {
    private RaftServer? _raft;
    private readonly ILogger<RailServer> _logger = loggerFactory.CreateLogger<RailServer>();
    private readonly TcpListener _listener = new(config.EndPoint);
    private readonly CancellationTokenSource _cts = new();
    private readonly DataStore _dataStore = new(config.DataDir);
    private readonly static long SuperSecret = 0XB3415C00L;

    private readonly SessionTracker _sessionTracker = new(TimeSpan.FromMilliseconds(config.Tick),
        loggerFactory.CreateLogger<SessionTracker>());

    public record Config {
        public required string DataDir;
        public required IPEndPoint EndPoint;
        public readonly TimeSpan ReceiveTimeout = TimeSpan.FromSeconds(6);
        public readonly int MinSessionTimeout = 1000;
        public readonly int MaxSessionTimeout = 10 * 1000;
        public readonly int Tick = 1000;
    }

    public void Start() {
        _listener.Start();
        Task.Run(async () => {
            _logger.LogInformation("Rail server started");
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
        loggerFactory.Dispose();
    }

    private async Task Broadcast(BroadcastRequest request) {
        if (_raft == null) {
            _logger.LogWarning("Raft server is not running, can't broadcast requests");
            return;
        }
        using var ms = new MemoryStream();
        request.SerializeTo(ms);
        var buffer = ms.ToArray();
        _raft.AddEntryApplied(buffer);
        await Task.CompletedTask;
    }

    private async Task HandleConnection(TcpClient client, CancellationToken cancellationToken) {
        // by now, only leader can handle connections
        _logger.LogInformation("Incoming client connection");
        await using var clientStream = client.GetStream();
        clientStream.ReadTimeout = config.ReceiveTimeout.Milliseconds;

        var connectRequest = await ReceiveRequest<ConnectRequest>(clientStream, cancellationToken);
        var connectResponse = CreateSession(connectRequest);
        var sessionId = connectResponse.SessionId;
        await SendResponse(clientStream, connectResponse, cancellationToken);

        // we're good, handle client requests now
        var closing = false;
        while (!closing) {
            var requestHeader = await ReceiveRequest<RequestHeader>(clientStream, cancellationToken);
            var requestType = requestHeader.Type.ToEnum();
            if (requestType == null) {
                throw new Exception($"Invalid request type: {requestHeader.Type}");
            }
            switch (requestType) {
                case OpCode.Notification: // do nothing...
                    break;
                case OpCode.Create: {
                    var request = await ReceiveRequest<CreateRequest>(clientStream, cancellationToken);
                    await Broadcast(new BroadcastRequest(requestHeader, JuteSerializer.Serialize(request)));
                    break;
                }
                case OpCode.Delete: {
                    var request = await ReceiveRequest<DeleteRequest>(clientStream, cancellationToken);
                    await Broadcast(new BroadcastRequest(requestHeader, JuteSerializer.Serialize(request)));
                    break;
                }
                case OpCode.Exists: {
                    var request = await ReceiveRequest<ExistsRequest>(clientStream, cancellationToken);
                    Stat? stat = null;
                    if (request.Path != null) {
                        stat = _dataStore.Exists(request.Path);
                    }
                    await SendResponse(clientStream, new ExistsResponse { Stat = stat }, cancellationToken);
                    break;
                }
                case OpCode.GetData:
                    break;
                case OpCode.SetData:
                    break;
                case OpCode.GetACL:
                    break;
                case OpCode.SetACL:
                    break;
                case OpCode.GetChildren:
                    break;
                case OpCode.Sync:
                    break;
                case OpCode.Ping: {
                    _sessionTracker.Touch(sessionId);
                    break;
                }
                case OpCode.GetChildren2:
                    break;
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
                case OpCode.CreateSession:
                    break;
                case OpCode.CloseSession:
                    _sessionTracker.Remove(sessionId);
                    closing = true;
                    break;
                case OpCode.Error:
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
        _logger.LogInformation("Clinet connection closed");
        await Task.CompletedTask;
    }

    private ConnectResponse CreateSession(ConnectRequest request) {
        _logger.LogInformation("Client attempting to establish new session");
        var sessionTimeout = int.Min(int.Max(request.Timeout, config.MinSessionTimeout), config.MaxSessionTimeout);
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

    private async Task<T> ReceiveRequest<T>(NetworkStream stream, CancellationToken cancellationToken)
        where T : IJuteDeserializable, new() {
        var buf = new byte[sizeof(int)];
        await stream.ReadExactlyAsync(buf, 0, buf.Length, cancellationToken);
        var packetLength = BinaryPrimitives.ReadInt32BigEndian(buf);

        buf = new byte[packetLength];
        await stream.ReadExactlyAsync(buf, 0, packetLength, cancellationToken);
        return JuteDeserializer.Deserialize<T>(buf);
    }

    private async Task SendResponse<T>(NetworkStream stream, T message, CancellationToken cancellationToken)
        where T : IJuteSerializable {
        byte[] buffer;
        using (var ms = new MemoryStream()) {
            JuteSerializer.SerializeTo(ms, message);
            buffer = ms.ToArray();
        }
        JuteSerializer.SerializeTo(stream, buffer.Length);
        await stream.WriteAsync(buffer, 0, buffer.Length, cancellationToken);
    }

    public void Apply(List<Command> commands) {
        foreach (var command in commands) {
            var request = new BroadcastRequest(command.Buffer.Memory);
            var requestType = request.Type;
            if (requestType == null) {
                _logger.LogInformation("invalid request type");
                continue;
            }
            switch (requestType) {
                case OpCode.Notification:
                    break;
                case OpCode.Create:
                    break;
                case OpCode.Delete:
                    break;
                case OpCode.Exists:
                    break;
                case OpCode.GetData:
                    break;
                case OpCode.SetData:
                    break;
                case OpCode.GetACL:
                    break;
                case OpCode.SetACL:
                    break;
                case OpCode.GetChildren:
                    break;
                case OpCode.Sync:
                    break;
                case OpCode.Ping:
                    break;
                case OpCode.GetChildren2:
                    break;
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
                case OpCode.CreateSession:
                    break;
                case OpCode.CloseSession:
                    break;
                case OpCode.Error:
                    break;
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