using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using FastRail.Jutes;
using FastRail.Jutes.Proto;
using Microsoft.Extensions.Logging;

namespace FastRail.Server;

public class RailServer(IPEndPoint endPoint, RailServer.Config config, ILoggerFactory loggerFactory) : IDisposable {
    private readonly ILogger<RailServer> _logger = loggerFactory.CreateLogger<RailServer>();
    private readonly TcpListener _listener = new(endPoint);
    private readonly CancellationTokenSource _cts = new();
    private readonly static long SuperSecret = 0XB3415C00L;

    private readonly SessionTracker _sessionTracker =
        new(TimeSpan.FromMilliseconds(config.Tick), loggerFactory.CreateLogger<SessionTracker>());

    public record Config {
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

    private async Task HandleConnection(TcpClient client, CancellationToken cancellationToken) {
        _logger.LogInformation("Incoming client connection");
        await using var stream = client.GetStream();
        stream.ReadTimeout = config.ReceiveTimeout.Milliseconds;

        var connectRequest = await ReceiveRequest<ConnectRequest>(stream, cancellationToken);
        var connectResponse = CreateSession(connectRequest);
        var sessionId = connectResponse.SessionId;
        await SendResponse(stream, connectResponse, cancellationToken);

        // we're good, handle client requests now
        var closing = false;
        while (!closing) {
            var requestHeader = await ReceiveRequest<RequestHeader>(stream, cancellationToken);
            if (requestHeader.Type == (int)OpCode.Ping) {
                _sessionTracker.Touch(sessionId);
            } else if (requestHeader.Type == (int)OpCode.CloseSession) {
                _sessionTracker.Remove(sessionId);
                closing = true;
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
}