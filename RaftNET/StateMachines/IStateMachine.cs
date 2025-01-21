using RaftNET.Services;

namespace RaftNET.StateMachines;

public interface IStateMachine {
    void Apply(List<Command> commands);
    void DropSnapshot(ulong snapshot);
    void LoadSnapshot(ulong snapshot);
    void OnEvent(Event e);
    ulong TakeSnapshot();
    void TransferSnapshot(ulong from, SnapshotDescriptor snapshot);
}
