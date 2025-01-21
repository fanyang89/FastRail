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
    private readonly Dictionary<ulong, bool> _alive = new();
    private readonly Dictionary<ulong, CancellationTokenSource> _cancellationTokenSources = new();
    private readonly ConnectionManager _connectionManager = new(myId, addressBook);
    private readonly Dictionary<ulong, Task> _workers = new();

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
            Log.Information("[{my_id}] PingWorker started, to={to}", myId, to);
            var lastAlive = clock.Now;
            while (!cancellationToken.IsCancellationRequested) {
                var deadline = clock.Now + timeout;
                try {
                    await _connectionManager.PingAsync(to, deadline, cancellationToken);
                    lastAlive = clock.Now;
                }
                catch (RpcException ex) {
                    Log.Error("[{my_id}] Ping failed, to={to} code={code} detail={detail}", myId, to, ex.StatusCode,
                        ex.Status.Detail);
                }
                lock (_alive) {
                    _alive[to] = clock.Now - lastAlive < timeout;
                }
                await Task.Delay(interval, cancellationToken);
            }
            Log.Information("[{my_id}] PingWorker exiting, to={to}", myId, to);
        }, cancellationToken);
    }
}
