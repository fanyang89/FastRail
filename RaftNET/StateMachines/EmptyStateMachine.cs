namespace RaftNET.StateMachines;

public class EmptyStateMachine : IStateMachine {
    public void Apply(List<Command> commands) {
    }

    public ulong TakeSnapshot() {
        return 0;
    }

    public void DropSnapshot(ulong snapshot) {
    }

    public void LoadSnapshot(ulong snapshot) {
    }
}