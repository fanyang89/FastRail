using Microsoft.Extensions.Logging;
using RaftNET.Services;
using RaftNET.StateMachines;

namespace RaftNET.Examples;

class InMemory(ulong myId, ILogger<InMemory> logger) : IStateMachine {
    public void Apply(List<Command> commands) {
        foreach (var command in commands) {
            logger.LogInformation("[{}] Applying command: {}", myId, command.Buffer);
        }
    }

    public ulong TakeSnapshot() {
        const ulong id = 123;
        logger.LogInformation("[{}] TakeSnapshot() id={}", myId, id);
        return id;
    }

    public void DropSnapshot(ulong snapshot) {
        logger.LogInformation("[{}] DropSnapshot() snapshot={}", myId, snapshot);
    }

    public void LoadSnapshot(ulong snapshot) {
        logger.LogInformation("[{}] LoadSnapshot() snapshot={}", myId, snapshot);
    }

    public void TransferSnapshot(ulong from, SnapshotDescriptor snapshot) {
        logger.LogInformation("[{}] TransferSnapshot() from={} snapshot={}", myId, from, snapshot);
    }

    public void OnEvent(Event e) {
        if (e.IsT0) {
            logger.LogInformation("Role change, role={} server_id={}", e.AsT0.Role, e.AsT0.ServerId);
        }
    }
}
