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
        dataDirOption.AddAlias("-d");
        rootCommand.AddOption(dataDirOption);

        var myIdOption = new Option<ulong>(
            name: "--my-id",
            description: "The ID for this server") { IsRequired = true };
        myIdOption.AddAlias("-i");
        rootCommand.AddOption(myIdOption);

        var initialmemberOption = new Option<List<string>>(
            name: "--members",
            description: "Initial members, eg. 1=127.0.0.1:3000") {
            IsRequired = true,
            AllowMultipleArgumentsPerToken = true
        };
        initialmemberOption.AddAlias("-m");
        rootCommand.AddOption(initialmemberOption);

        rootCommand.SetHandler(
            Run(args, loggerFactory),
            dataDirOption, myIdOption, initialmemberOption
        );
        return await rootCommand.InvokeAsync(args);
    }

    private static Action<DirectoryInfo?, ulong, List<string>> Run(string[] args, ILoggerFactory loggerFactory) {
        return (dataDir, myId, initialMembers) => {
            if (dataDir == null) {
                throw new ArgumentException(nameof(dataDir));
            }
            if (myId == 0) {
                throw new ArgumentException(nameof(myId));
            }

            var builder = WebApplication.CreateBuilder(args);
            builder.Services.AddHostedService<RaftService>(_ => new RaftService(
                new RaftService.Config(
                    MyId: myId,
                    DataDir: dataDir.FullName,
                    LoggerFactory: loggerFactory,
                    StateMachine: new LogStateMachine(loggerFactory.CreateLogger<LogStateMachine>()),
                    AddressBook: new AddressBook(initialMembers)
                )));
            builder.Services.AddSingleton<RaftService>(
                provider => provider.GetServices<IHostedService>().OfType<RaftService>().First());
            builder.Services.AddGrpc();

            var app = builder.Build();
            app.MapGrpcService<RaftService>();
            app.MapGet("/", () => "Raft.NET example service");
            app.Run();
        };
    }
}