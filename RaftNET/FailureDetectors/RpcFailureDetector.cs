using Microsoft.Extensions.Logging;
using RaftNET.Services;

namespace RaftNET.FailureDetectors;

public class RpcFailureDetector(
    ulong myId,
    AddressBook addressBook,
    IClock clock,
    ILoggerFactory loggerFactory,
    TimeSpan interval,
    TimeSpan timeout
) : IFailureDetector, IListener, IDisposable {
    private readonly Dictionary<ulong, IPingWorker> _workers = new();
    private readonly Dictionary<ulong, bool> _alive = new();

    public void Dispose() {
        RemoveAll();
    }

    public bool IsAlive(ulong server) {
        lock (_alive) {
            return _alive.GetValueOrDefault(server, false);
        }
    }

    public void MarkAlive(ulong server) {
        lock (_alive) {
            _alive[server] = true;
        }
    }

    public void MarkDead(ulong server) {
        lock (_alive) {
            _alive[server] = false;
        }
    }

    public virtual IPingWorker CreateWorker(ulong serverId) {
        return new PingWorker(myId, serverId, addressBook, interval, timeout,
            loggerFactory.CreateLogger<PingWorker>(), this, clock);
    }

    public void Add(ulong serverId) {
        lock (_workers) {
            var worker = CreateWorker(serverId);
            _workers.Add(serverId, worker);
            worker.Start();
        }
    }

    public void Remove(ulong serverId) {
        lock (_workers) {
            _workers.Remove(serverId, out var worker);

            if (worker != null) {
                worker.Stop();
            }
        }
    }

    public void RemoveAll() {
        lock (_workers) {
            foreach (var (_, worker) in _workers) {
                worker.Stop();
            }

            _workers.Clear();
        }
    }

    public void UpdateAddress() {
        lock (_workers) {
            foreach (var (_, worker) in _workers) {
                worker.UpdateAddress();
            }
        }
    }
}