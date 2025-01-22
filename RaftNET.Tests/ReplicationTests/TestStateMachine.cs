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
    private readonly SemaphoreSlim _done = new(0, 1);
    private readonly Dictionary<ulong, Dictionary<ulong, SnapshotValue>> _snapshots = snapshots;
    private ulong _seen;

    public HasherInt Hasher { get; private set; } = new();

    public void Apply(List<Command> commands) {
        var n = apply(id, commands, Hasher);
        _seen += (ulong)n;
        if (_seen >= applyEntries) {
            if (_seen > applyEntries) {
                Log.Warning("[{my_id}] Apply() seen overshot apply entries, seen={seen} apply_entries={apply_entries}",
                    id, _seen, applyEntries);
            }
            _done.Release(1);
        }
        Log.Debug("[{my_id}] Apply() got {seen}/{apply_entries} entries", id, _seen, applyEntries);
    }

    public void DropSnapshot(ulong snapshotId) {
        _snapshots[id].Remove(snapshotId);
    }

    public void LoadSnapshot(ulong snapshotId) {
        var snapshot = _snapshots[id][snapshotId];
        Hasher = snapshot.Hasher;
        var hash = Hasher.FinalizeUInt64();
        Log.Debug("[{my_id}] LoadSnapshot(), id={id} idx={idx} hash={hash}", id, snapshotId, snapshot.Idx, hash);
        _seen = snapshot.Idx;
        if (_seen >= applyEntries) {
            _done.Release(1);
        }
        // if (snapshotId == delay) {}
    }

    public void OnEvent(Event e) {
        e.Switch(ev => {
            Log.Information("[{my_id}] Role changed, role={role}, id={id}", id, ev.Role, ev.ServerId);
        });
    }

    public ulong TakeSnapshot() {
        throw new NotImplementedException();
    }

    public void TransferSnapshot(ulong from, SnapshotDescriptor snapshot) {
        Log.Information("[{my_id}] Should transfer snapshot, from={from} id={id}", id, from, snapshot.Id);
    }

    public async Task DoneAsync() {
        await _done.WaitAsync();
    }
}
