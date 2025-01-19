using RaftNET.Services;

namespace RaftNET.StateMachines;

public interface IStateMachine {
    void Apply(List<Command> commands);
    ulong TakeSnapshot();
    void DropSnapshot(ulong snapshot);
    void LoadSnapshot(ulong snapshot);
    void TransferSnapshot(ulong from, SnapshotDescriptor snapshot);
    void OnEvent(Event e);
}
