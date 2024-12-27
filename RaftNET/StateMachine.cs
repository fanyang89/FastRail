namespace RaftNET;

public interface IStateMachine {
    void Apply(IList<Command> commands);
    ulong TakeSnapshot();
    void DropSnapshot();
    void LoadSnapshot(ulong snapshot);
}