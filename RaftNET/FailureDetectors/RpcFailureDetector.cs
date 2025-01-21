using Grpc.Core;
using RaftNET.Services;
using Serilog;

namespace RaftNET.FailureDetectors;

public class RpcFailureDetector(
    ulong myId,
    AddressBook addressBook,
    TimeSpan interval,
    TimeSpan timeout,
    IClock clock
) : IFailureDetector {
    private readonly Dictionary<ulong, CancellationTokenSource> _cancellationTokenSources = new();
    private readonly Dictionary<ulong, Task> _workers = new();
    private readonly Dictionary<ulong, bool> _alive = new();
    private readonly ConnectionManager _connectionManager = new(myId, addressBook);

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
            Log.Information("[{}] PingWorker started, to={}", myId, to);
            var lastAlive = clock.Now;
            while (!cancellationToken.IsCancellationRequested) {
                var deadline = clock.Now + timeout;
                try {
                    await _connectionManager.PingAsync(to, deadline, cancellationToken);
                    lastAlive = clock.Now;
                }
                catch (RpcException ex) {
                    Log.Error("[{}] Ping failed, to={} code={} detail={}", myId, to, ex.StatusCode, ex.Status.Detail);
                }
                lock (_alive) {
                    _alive[to] = clock.Now - lastAlive < timeout;
                }
                await Task.Delay(interval, cancellationToken);
            }
            Log.Information("[{}] PingWorker exiting, to={}", myId, to);
        }, cancellationToken);
    }
}
