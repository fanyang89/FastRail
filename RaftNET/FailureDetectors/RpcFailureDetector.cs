using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RaftNET.Services;

namespace RaftNET.FailureDetectors;

public class RpcFailureDetector(
    ulong myId,
    AddressBook addressBook,
    TimeSpan interval,
    TimeSpan timeout,
    IClock clock,
    ILogger<RpcFailureDetector>? logger
) : IFailureDetector {
    private readonly Dictionary<ulong, CancellationTokenSource> _cancellationTokenSources = new();
    private readonly Dictionary<ulong, Task> _workers = new();
    private readonly Dictionary<ulong, bool> _alive = new();
    private readonly ILogger<RpcFailureDetector> _logger = logger ?? new NullLogger<RpcFailureDetector>();
    private readonly ConnectionManager _connectionManager = new(myId, addressBook);
    private readonly IClock _clock = clock;

    public bool IsAlive(ulong server) {
        lock (_alive) {
            return _alive.GetValueOrDefault(server, false);
        }
    }

    public void Add(ulong serverId) {
        lock (_workers)
        lock (_cancellationTokenSources) {
            var cts = new CancellationTokenSource();
            _workers.Add(serverId, WorkerAsync(serverId, cts.Token));
            _cancellationTokenSources.Add(serverId, cts);
        }
    }

    public void Remove(ulong serverId) {
        lock (_workers)
        lock (_cancellationTokenSources) {
            _cancellationTokenSources.Remove(serverId, out var cts);
            cts?.Cancel();
            _workers.Remove(serverId);
        }
    }

    private Task WorkerAsync(ulong to, CancellationToken cancellationToken) {
        return Task.Run(async delegate {
            _logger.LogInformation("[{}] PingWorker started, to={}", myId, to);
            var lastAlive = _clock.Now;
            while (!cancellationToken.IsCancellationRequested) {
                var deadline = _clock.Now + timeout;
                try {
                    await _connectionManager.PingAsync(to, deadline, cancellationToken);
                    lastAlive = _clock.Now;
                }
                catch (RpcException ex) {
                    _logger.LogError("[{}] Ping failed, to={} code={} detail={}", myId, to, ex.StatusCode, ex.Status.Detail);
                }
                lock (_alive) {
                    _alive[to] = _clock.Now - lastAlive < timeout;
                }
                await Task.Delay(interval, cancellationToken);
            }
            _logger.LogInformation("[{}] PingWorker exiting, to={}", myId, to);
        }, cancellationToken);
    }
}
