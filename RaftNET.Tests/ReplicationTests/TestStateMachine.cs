using RaftNET.Services;
using RaftNET.StateMachines;
using Serilog;

namespace RaftNET.Tests.ReplicationTests;

public sealed class TestStateMachine(
    ulong id,
    ApplyFn apply,
    ulong applyEntries,
    Snapshots snapshots
) : IStateMachine {
    private Dictionary<ulong, Dictionary<ulong, SnapshotValue>> _snapshots = snapshots;
    private ulong _applyEntries = applyEntries;
    private ulong _seen;
    private ulong _id = id;
    private readonly SemaphoreSlim _done = new(1, 1);

    public HasherInt Hasher { get; set; }

    public void Apply(List<Command> commands) {
        var n = apply(_id, commands, Hasher);
        _seen += (ulong)n;
        if (n > 0 && _seen >= _applyEntries) {
            if (_seen > _applyEntries) {
                Log.Warning("[{}] Apply() seen overshot apply entries, seen={} apply_entries={}",
                    _id, _seen, _applyEntries);
            }
            _done.Release(1);
        }
        Log.Debug("[{}] Apply() got {}/{} entries", _id, _seen, _applyEntries);
    }

    public ulong TakeSnapshot() {
        throw new NotImplementedException();
    }

    public void DropSnapshot(ulong snapshotId) {
        _snapshots[_id].Remove(snapshotId);
    }

    public void LoadSnapshot(ulong snapshotId) {
        var snapshot = _snapshots[_id][snapshotId];
        Hasher = snapshot.Hasher;
        var hash = Hasher.FinalizeUInt64();
        Log.Debug("[{}] LoadSnapshot(), id={} idx={} hash={}", _id, snapshotId, snapshot.Idx, hash);
        _seen = snapshot.Idx;
        if (_seen >= _applyEntries) {
            _done.Release(1);
        }
        // if (snapshotId == delay) {}
    }

    public void TransferSnapshot(ulong from, SnapshotDescriptor snapshot) {
        Log.Information("[{}] Should transfer snapshot, from={} id={}", _id, from, snapshot.Id);
    }

    public void OnEvent(Event e) {
        e.Switch(ev => {
            Log.Information("[{}] Role changed, role={}, id={}", _id, ev.Role, ev.ServerId);
        });
    }

    public async Task DoneAsync() {
        await _done.WaitAsync(1);
    }
}
