using System.CommandLine;
using RaftNET.Services;

namespace RaftNET.Examples;

class Program {
    static async Task<int> Main(string[] args) {
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
            (dataDir, myId) => {
                if (dataDir == null) {
                    throw new ArgumentException(nameof(dataDir));
                }

                var builder = WebApplication.CreateBuilder(args);
                builder.Services.AddSingleton<IPersistence, RocksPersistence>(CreatePersistence(dataDir));
                builder.Services.AddSingleton(CreateLog);
                builder.Services.AddSingleton(CreateFSM(myId));
                builder.Services.AddGrpc();

                var app = builder.Build();
                app.MapGrpcService<RaftService>();
                app.MapGet("/", () => "Raft.NET example service");
                app.Run();
            },
            dataDirOption, myIdOption);

        return await rootCommand.InvokeAsync(args);
    }

    private static Func<IServiceProvider, RocksPersistence> CreatePersistence(DirectoryInfo dataDir) {
        return provider => new RocksPersistence(dataDir.FullName);
    }

    private static Func<IServiceProvider, FSM> CreateFSM(ulong myId) {
        return provider => {
            var persistence = provider.GetService<IPersistence>();
            var logger = provider.GetService<ILogger<FSM>>();
            var log = provider.GetService<Log>();
            if (persistence == null || logger == null || log == null) {
                throw new ArgumentNullException();
            }

            ulong term = 0;
            ulong votedFor = 0;
            var tv = persistence.LoadTermVote();
            if (tv != null) {
                term = tv.Term;
                votedFor = tv.VotedFor;
            }
            var commitIdx = persistence.LoadCommitIdx();
            return new FSM(
                myId, term, votedFor, log, commitIdx,
                new TrivialFailureDetector(),
                new FSMConfig(),
                logger
            );
        };
    }

    private static Func<IServiceProvider, Log> CreateLog() {
        return provider => {
            var persistence = provider.GetService<IPersistence>();
            var logger = provider.GetService<ILogger<Log>>();
            if (persistence == null || logger == null) {
                throw new ArgumentNullException();
            }

            var snapshot = persistence.LoadSnapshotDescriptor();
            var logEntries = persistence.LoadLog();
            return new Log(snapshot, logEntries, logger);
        };
    }
}