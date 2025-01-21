using RaftNET.Services;
using RaftNET.StateMachines;
using Serilog;

namespace RaftNET.Examples;

class InMemory(ulong myId) : IStateMachine {
    public void Apply(List<Command> commands) {
        foreach (var command in commands) {
            Log.Information("[{my_id}] Applying command: {buffer}", myId, command.Buffer);
        }
    }

    public ulong TakeSnapshot() {
        const ulong id = 123;
        Log.Information("[{my_id}] TakeSnapshot() id={id}", myId, id);
        return id;
    }

    public void DropSnapshot(ulong snapshot) {
        Log.Information("[{my_id}] DropSnapshot() snapshot={snapshot}", myId, snapshot);
    }

    public void LoadSnapshot(ulong snapshot) {
        Log.Information("[{my_id}] LoadSnapshot() snapshot={snapshot}", myId, snapshot);
    }

    public void TransferSnapshot(ulong from, SnapshotDescriptor snapshot) {
        Log.Information("[{my_id}] TransferSnapshot() from={from} snapshot={snapshot}", myId, from, snapshot);
    }

    public void OnEvent(Event e) {
        if (e.IsT0) {
            Log.Information("Role change, role={role} server_id={server_id}", e.AsT0.Role, e.AsT0.ServerId);
        }
    }
}
