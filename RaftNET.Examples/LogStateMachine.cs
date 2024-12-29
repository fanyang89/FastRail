using RaftNET.Services;
using RaftNET.StateMachines;

namespace RaftNET.Examples;

class LogStateMachine(ILogger<LogStateMachine> logger) : IStateMachine {
    public void Apply(List<Command> commands) {
        foreach (var command in commands) {
            logger.LogInformation("Applying command: {}", command.Buffer);
        }
    }

    public ulong TakeSnapshot() {
        logger.LogInformation("Taking snapshot");
        return 1;
    }

    public void DropSnapshot(ulong snapshot) {
        logger.LogInformation("Drop snapshot");
    }

    public void LoadSnapshot(ulong snapshot) {
        logger.LogInformation("Loading snapshot");
    }

    public void OnEvent(Event ev) {
        ev.Switch(e => {
            logger.LogInformation("Server({}) role change to {}", e.ServerId, e.Role);
        });
    }
}