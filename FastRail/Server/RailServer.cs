﻿using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using FastRail.Jutes;
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
    private readonly DataStore _ds = new(config.DataDir);
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

    private async Task HandleConnection(TcpClient client, CancellationToken token) {
        // by now, only leader can handle connections
        _logger.LogInformation("Incoming client connection");
        await using var conn = client.GetStream();
        conn.ReadTimeout = config.ReceiveTimeout.Milliseconds;

        var connectRequest = await ReceiveConnectRequest(conn, token);
        var connectResponse = CreateSession(connectRequest);
        var sessionId = connectResponse.SessionId;
        await SendConnectResponse(conn, token, connectResponse);

        // we're good, handle client requests now
        var closing = false;

        while (!closing) {
            var (requestHeader, requestBuffer) = await ReceiveRequest(conn, token);
            var requestType = requestHeader.Type.ToEnum();
            var xid = requestHeader.Xid;

            if (requestType == null) {
                throw new Exception($"Invalid request type: {requestHeader.Type}");
            }

            try {
                switch (requestType) {
                    case OpCode.Notification: // do nothing...
                        break;
                    case OpCode.Create: {
                        var request = JuteDeserializer.Deserialize<CreateRequest>(requestBuffer);
                        await Broadcast(new BroadcastRequest(requestHeader, JuteSerializer.Serialize(request)));
                        await SendResponse(conn, token, new ReplyHeader(xid, _ds.LastZxid),
                            new CreateResponse { Path = request.Path });
                        break;
                    }
                    case OpCode.Delete: {
                        var request = JuteDeserializer.Deserialize<DeleteRequest>(requestBuffer);
                        await Broadcast(new BroadcastRequest(requestHeader, JuteSerializer.Serialize(request)));
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
                        await Broadcast(new BroadcastRequest(requestHeader, JuteSerializer.Serialize(request)));
                        break;
                    }
                    case OpCode.GetACL:
                        break;
                    case OpCode.SetACL: {
                        var request = JuteDeserializer.Deserialize<SetACLRequest>(requestBuffer);
                        await Broadcast(new BroadcastRequest(requestHeader, JuteSerializer.Serialize(request)));
                        break;
                    }
                    case OpCode.GetChildren:
                        break;
                    case OpCode.Sync: {
                        var request = JuteDeserializer.Deserialize<SyncRequest>(requestBuffer);
                        await Broadcast(new BroadcastRequest(requestHeader, JuteSerializer.Serialize(request)));
                        break;
                    }
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

    private async Task SendResponse<T>(NetworkStream stream, CancellationToken token, ReplyHeader header, T? response)
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
        await stream.WriteAsync(lenBuffer, 0, lenBuffer.Length, token);
        await stream.WriteAsync(headerBuffer, 0, headerBuffer.Length, token);
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