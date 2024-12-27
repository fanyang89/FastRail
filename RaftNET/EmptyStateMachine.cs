namespace RaftNET;

public class EmptyStateMachine : IStateMachine {
    public void Apply(IList<Command> commands) {}

    public ulong TakeSnapshot() {
        return 0;
    }

    public void DropSnapshot() {}

    public void LoadSnapshot(ulong snapshot) {}
}