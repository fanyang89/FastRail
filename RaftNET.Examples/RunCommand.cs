using System.Net;
using Microsoft.Extensions.Logging;
using RaftNET.FailureDetectors;
using RaftNET.Persistence;
using RaftNET.Services;
using Spectre.Console;
using Spectre.Console.Cli;

namespace RaftNET.Examples;

class RunCommand : Command<RunCommand.Settings> {
    public class Settings : CommandSettings {
        [CommandArgument(0, "<ID>")] public required ulong MyId { get; init; }
        [CommandArgument(1, "<LISTEN>")] public required string Listen { get; init; }
        [CommandArgument(2, "<MEMBERS>")] public required string[] Members { get; init; }
        [CommandOption("-d|--data-dir")] public string? DataDir { get; set; }
        [CommandOption("-t|--runtime")] public TimeSpan Runtime { get; init; } = TimeSpan.FromSeconds(10);
        [CommandOption("--ping-interval")] public TimeSpan PingInterval { get; init; } = TimeSpan.FromMilliseconds(500);
        [CommandOption("--ping-timeout")] public TimeSpan PingTimeout { get; init; } = TimeSpan.FromMilliseconds(1000);
    }

    public override ValidationResult Validate(CommandContext context, Settings settings) {
        if (!IPEndPoint.TryParse(settings.Listen, out _)) {
            return ValidationResult.Error("Invalid listen address");
        }
        if (settings.MyId == 0) {
            return ValidationResult.Error("Invalid id");
        }
        if (settings.Members.Length == 0) {
            return ValidationResult.Error("No initial members");
        }
        return ValidationResult.Success();
    }

    private async Task<int> ExecuteAsync(CommandContext context, Settings settings) {
        var logger = LoggerFactory.Instance.CreateLogger<RunCommand>();
        if (string.IsNullOrEmpty(settings.DataDir)) {
            settings.DataDir = Directory.CreateTempSubdirectory("RaftNET.Examples").FullName;
        }

        var sm = new InMemory(settings.MyId, LoggerFactory.Instance.CreateLogger<InMemory>());
        var addressBook = new AddressBook(settings.Members.ToList());
        var loggerFactory = LoggerFactory.Instance;
        var rpcClient = new ConnectionManager(settings.MyId, addressBook, loggerFactory.CreateLogger<ConnectionManager>());
        var persistence = new RocksPersistence(settings.DataDir);
        var clock = new SystemClock();
        var fd = new RpcFailureDetector(settings.MyId, addressBook,
            settings.PingInterval, settings.PingTimeout,
            clock, loggerFactory.CreateLogger<RpcFailureDetector>());
        var service = new RaftService(settings.MyId, rpcClient, sm, persistence, fd, addressBook, loggerFactory,
            new RaftServiceOptions());
        var listen = IPEndPoint.Parse(settings.Listen);
        var server = new RaftServer(service, listen.Address, listen.Port);
        _ = server.Start();
        await Task.Delay(settings.Runtime);
        logger.LogInformation("Runtime timeout, exiting...");
        server.Stop();
        return 0;
    }

    public override int Execute(CommandContext context, Settings settings) {
        return ExecuteAsync(context, settings).Result;
    }
}
