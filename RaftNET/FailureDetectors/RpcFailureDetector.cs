using Microsoft.Extensions.Logging;
using RaftNET.Services;

namespace RaftNET.FailureDetectors;

public class RpcFailureDetector(
    ulong myId,
    AddressBook addressBook,
    TimeSpan interval,
    TimeSpan timeout,
    ILoggerFactory loggerFactory
) : IFailureDetector, IListener {
    private readonly Dictionary<ulong, PingWorker> _workers = new();
    private readonly Dictionary<ulong, bool> _alive = new();

    public bool IsAlive(ulong server) {
        lock (_alive) {
            return _alive.GetValueOrDefault(server, false);
        }
    }

    public void Add(ulong serverId) {
        lock (_workers) {
            _workers.Add(serverId,
                new PingWorker(myId, serverId, addressBook, interval, timeout,
                    loggerFactory.CreateLogger<PingWorker>(), this, new SystemClock())
            );
        }
    }

    public void Remove(ulong serverId) {
        lock (_workers) {
            _workers.Remove(serverId, out var worker);
            if (worker != null) {
                worker.Dispose();
            }
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

    public void UpdateAddress() {
        lock (_workers) {
            foreach (var (_, worker) in _workers) {
                worker.UpdateAddress();
            }
        }
    }
}