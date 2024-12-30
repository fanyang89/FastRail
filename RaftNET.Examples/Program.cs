using System.CommandLine;
using System.Net;
using RaftNET.Services;
using Option = Google.Protobuf.WellKnownTypes.Option;

namespace RaftNET.Examples;

class Program {
    private static async Task<int> Main(string[] args) {
        using var loggerFactory = LoggerFactory.Instance;
        var rootCommand = new RootCommand("Example for Raft.NET");

        var dataDirOption = new Option<DirectoryInfo?>(
            "--data-dir",
            "The data directory") { IsRequired = true };
        dataDirOption.AddAlias("-d");
        rootCommand.AddOption(dataDirOption);

        var myIdOption = new Option<ulong>(
            "--my-id",
            "The ID for this server") { IsRequired = true };
        myIdOption.AddAlias("-i");
        rootCommand.AddOption(myIdOption);

        var listenAddressOption = new Option<IPAddress>(
            "--listen-address",
            "The listen address") { IsRequired = true };
        listenAddressOption.AddAlias("-l");
        rootCommand.AddOption(listenAddressOption);

        var listenPortOption = new Option<int>(
            "--port",
            "The listen port") { IsRequired = true };
        listenPortOption.AddAlias("-p");
        rootCommand.AddOption(listenPortOption);

        var initialmemberOption = new Option<List<string>>(
            "--members",
            "Initial members, eg. 1=127.0.0.1:3000") {
            IsRequired = true,
            AllowMultipleArgumentsPerToken = true
        };
        initialmemberOption.AddAlias("-m");
        rootCommand.AddOption(initialmemberOption);

        rootCommand.SetHandler(
            Run(args, loggerFactory),
            dataDirOption, myIdOption, initialmemberOption, listenAddressOption, listenPortOption
        );
        return await rootCommand.InvokeAsync(args);
    }

    private static Action<DirectoryInfo?, ulong, List<string>, IPAddress, int>
        Run(string[] args, ILoggerFactory loggerFactory) {
        return (dataDir, myId, initialMembers, listenAddress, listenPort) => {
            if (dataDir == null) {
                throw new ArgumentException(nameof(dataDir));
            }
            if (myId == 0) {
                throw new ArgumentException(nameof(myId));
            }

            var builder = WebApplication.CreateBuilder(args);
            var addressBook = new AddressBook(initialMembers);
            builder.Services.AddHostedService<RaftService>(_ => new RaftService(
                new RaftService.Config(
                    myId,
                    dataDir.FullName,
                    loggerFactory,
                    new LogStateMachine(loggerFactory.CreateLogger<LogStateMachine>()),
                    addressBook,
                    listenAddress,
                    listenPort
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