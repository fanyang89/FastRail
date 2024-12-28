using System.CommandLine;
using RaftNET.Services;

namespace RaftNET.Examples;

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
                new RaftService.Config {
                    DataDir = dataDir.FullName,
                    MyId = myId,
                    LoggerFactory = loggerFactory
                });
            builder.Services.AddGrpc();
            var app = builder.Build();
            app.MapGrpcService<RaftService>();
            app.MapGet("/", () => "Raft.NET example service");
            app.Run();
        };
    }
}