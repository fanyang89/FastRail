using System.CommandLine;
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

class Program {
    static async Task<int> Main(string[] args) {
        using var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
        var rootCommand = new RootCommand("Example for Raft.NET");

        var dataDirOption = new Option<DirectoryInfo?>(
            name: "--data-dir",
            description: "The data directory") { IsRequired = true };
        rootCommand.AddOption(dataDirOption);

        var myIdOption = new Option<ulong>(
            name: "--my-id",
            description: "The ID for this server") { IsRequired = true };
        rootCommand.AddOption(myIdOption);

        rootCommand.SetHandler(
            Run(args, loggerFactory),
            dataDirOption, myIdOption
        );
        return await rootCommand.InvokeAsync(args);
    }

    private static Action<DirectoryInfo?, ulong> Run(string[] args, ILoggerFactory loggerFactory) {
        return (dataDir, myId) => {
            if (dataDir == null) {
                throw new ArgumentException(nameof(dataDir));
            }
            if (myId == 0) {
                throw new ArgumentException(nameof(myId));
            }

            var builder = WebApplication.CreateBuilder(args);
            builder.Services.AddSingleton<RaftService.Config>(_ =>
                new RaftService.Config(
                    MyId: myId,
                    DataDir: dataDir.FullName,
                    LoggerFactory: loggerFactory,
                    StateMachine: new LogStateMachine(loggerFactory.CreateLogger<LogStateMachine>()),
                    AddressBook: new AddressBook()
                ));
            builder.Services.AddGrpc();
            var app = builder.Build();
            app.MapGrpcService<RaftService>();
            app.MapGet("/", () => "Raft.NET example service");
            app.Run();
        };
    }
}