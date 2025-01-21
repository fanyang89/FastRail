using RaftNET.Services;

namespace RaftNET.StateMachines;

public class EmptyStateMachine : IStateMachine {
    public void Apply(List<Command> commands) {}

    public void DropSnapshot(ulong snapshot) {}

    public void LoadSnapshot(ulong snapshot) {}

    public void OnEvent(Event e) {}

    public ulong TakeSnapshot() {
        return 0;
    }

    public void TransferSnapshot(ulong from, SnapshotDescriptor snapshot) {}
}
