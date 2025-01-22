using Grpc.Core;
using RaftNET.Services;
using Serilog;

namespace RaftNET.FailureDetectors;

public class RpcFailureDetector : IFailureDetector {
    private readonly Dictionary<ulong, bool> _alive = new();
    private readonly Dictionary<ulong, CancellationTokenSource> _cancellationTokenSources = new();
    private readonly ConnectionManagerProxy _connectionManager;
    private readonly ulong _myId;
    private readonly TimeSpan _interval;
    private readonly TimeSpan _timeout;
    private readonly IClock _clock;

    public RpcFailureDetector(ulong myId,
        AddressBook addressBook,
        TimeSpan interval,
        TimeSpan timeout,
        IClock clock) {
        _myId = myId;
        _interval = interval;
        _timeout = timeout;
        _clock = clock;
        _connectionManager = new(new ConnectionManager(myId, addressBook), this);
    }

    public bool IsAlive(ulong server) {
        lock (_alive) {
            return _alive.GetValueOrDefault(server, false);
        }
    }

    public void AddEndpoint(ulong serverId) {
        Add(serverId);
    }

    public void RemoveEndpoint(ulong serverId) {
        Remove(serverId);
    }

    public void Add(ulong serverId) {
        lock (_cancellationTokenSources) {
            var cts = new CancellationTokenSource();
            _ = WorkerAsync(serverId, cts.Token);
            _cancellationTokenSources.Add(serverId, cts);
        }
    }

    public void Remove(ulong serverId) {
        lock (_cancellationTokenSources) {
            _cancellationTokenSources.Remove(serverId, out var cts);
            cts?.Cancel();
        }
    }

    private Task WorkerAsync(ulong to, CancellationToken cancellationToken) {
        return Task.Run(async delegate {
            Log.Information("[{my_id}] PingWorker started, to={to}", _myId, to);
            var lastAlive = _clock.Now;
            while (!cancellationToken.IsCancellationRequested) {
                var deadline = _clock.Now + _timeout;
                try {
                    await _connectionManager.PingAsync(to, deadline, cancellationToken);
                    lastAlive = _clock.Now;
                }
                catch (RpcException ex) {
                    Log.Error("[{my_id}] Ping failed, to={to} code={code} detail={detail}", _myId, to, ex.StatusCode,
                        ex.Status.Detail);
                }
                lock (_alive) {
                    _alive[to] = _clock.Now - lastAlive < _timeout;
                }
                await Task.Delay(_interval, cancellationToken);
            }
            Log.Information("[{my_id}] PingWorker exiting, to={to}", _myId, to);
        }, cancellationToken);
    }
}
