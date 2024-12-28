namespace RaftNET;

public interface IStateMachine {
    void Apply(List<Command> commands);
    ulong TakeSnapshot();
    void DropSnapshot(ulong snapshot);
    void LoadSnapshot(ulong snapshot);
}