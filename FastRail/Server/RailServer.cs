using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using FastRail.Jutes;
using FastRail.Jutes.Proto;
using Microsoft.Extensions.Logging;

namespace FastRail.Server;

public class RailServer(IPEndPoint endPoint, RailServer.Config config, ILogger<RailServer> logger) {
    private readonly ILogger<RailServer> _logger = logger;
    private readonly TcpListener _listener = new(endPoint);
    private readonly CancellationTokenSource _cts = new();
    private readonly SessionTracker _sessionTracker = new();
    private readonly static long superSecret = 0XB3415C00L;

    public record Config {
        public TimeSpan ReceiveTimeout = TimeSpan.FromSeconds(6);
        public int MinSessionTimeout = 1000;
        public int MaxSessionTimeout = 10 * 1000;
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

    public async Task HandleConnection(TcpClient client, CancellationToken cancellationToken) {
        await using var stream = client.GetStream();
        stream.ReadTimeout = config.ReceiveTimeout.Milliseconds;

        var connectRequest = await ReceiveRequest<ConnectRequest>(stream, cancellationToken);
        var rsp = CreateSession(connectRequest);
        await SendResponse(stream, rsp, cancellationToken);

        await Task.CompletedTask;
    }


    public ConnectResponse CreateSession(ConnectRequest request) {
        _logger.LogInformation("Client attempting to establish new session");
        var sessionId = _sessionTracker.Add();
        var sessionTimeout = int.Min(int.Max(request.Timeout, config.MinSessionTimeout), config.MaxSessionTimeout);
        var passwd = request.Passwd ?? [];
        var rnd = new Random((int)(sessionId ^ superSecret));
        rnd.NextBytes(passwd);
        return new ConnectResponse {
            ProtocolVersion = 0,
            SessionId = sessionId,
            Passwd = passwd,
            Timeout = sessionTimeout,
            ReadOnly = false // not supported
        };
    }

    public async Task<T> ReceiveRequest<T>(NetworkStream stream, CancellationToken cancellationToken)
        where T : IJuteDeserializable, new() {
        var buf = new byte[sizeof(int)];
        await stream.ReadExactlyAsync(buf, 0, buf.Length, cancellationToken);
        var packetLength = BinaryPrimitives.ReadInt32BigEndian(buf);

        buf = new byte[packetLength];
        await stream.ReadExactlyAsync(buf, 0, packetLength, cancellationToken);
        return JuteDeserializer.Deserialize<T>(buf);
    }

    public async Task SendResponse<T>(NetworkStream stream, T message, CancellationToken cancellationToken)
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