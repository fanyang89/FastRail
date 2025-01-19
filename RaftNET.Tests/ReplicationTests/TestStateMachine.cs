using RaftNET.Services;
using RaftNET.StateMachines;

namespace RaftNET.Tests.ReplicationTests;

public sealed class TestStateMachine : IStateMachine {
    public ulong Id;
    public ApplyFn OnApply;
    public ulong ApplyEntries;
    public ulong Seen = 0;
    public Dictionary<ulong, Dictionary<ulong, SnapshotValue>> Snapshots;
    public HasherInt Hasher;

    public void Apply(List<Command> commands) {
        throw new NotImplementedException();
    }

    public ulong TakeSnapshot() {
        throw new NotImplementedException();
    }

    public void DropSnapshot(ulong snapshot) {
        throw new NotImplementedException();
    }

    public void LoadSnapshot(ulong snapshot) {
        throw new NotImplementedException();
    }

    public void TransferSnapshot(ulong from, SnapshotDescriptor snapshot) {
        throw new NotImplementedException();
    }

    public void OnEvent(Event e) {
        throw new NotImplementedException();
    }

    public async Task Done() {
        throw new NotImplementedException();
    }
}
