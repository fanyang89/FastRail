using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using FastRail.Jutes;
using FastRail.Jutes.Proto;
using Microsoft.Extensions.Logging;

namespace FastRail.Server;

public class ZooKeeperServer(IPEndPoint endPoint, ZooKeeperServer.Config config, ILogger<ZooKeeperServer> logger) {
    private readonly TcpListener _listener = new(endPoint);
    private readonly CancellationTokenSource _cts = new();

    public record Config {
        public TimeSpan ReceiveTimeout = TimeSpan.FromSeconds(6);
    }

    public void Start() {
        _listener.Start();
        Task.Run(async () => {
            logger.LogInformation("ZooKeeper server started");
            while (!_cts.Token.IsCancellationRequested) {
                var conn = await _listener.AcceptTcpClientAsync();
                _ = Task.Run(() => HandleConnection(conn, _cts.Token));
            }
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
        var sessionId = connectRequest.SessionId;
        var connectResponse = new ConnectResponse();
        await SendResponse(stream, connectResponse, cancellationToken);
        await Task.CompletedTask;
    }

    public void EnsureSession() {}

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