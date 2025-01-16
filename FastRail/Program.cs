using System.CommandLine;
using System.Net;
using RaftNET.Services;

namespace FastRail;

internal class Program {
    public static async Task Main(string[] args) {
        var rootCommand = new RootCommand(
            "A distributed key-value store compatible with the ZooKeeper protocol");

        var myIdOption = new Option<ulong>("--id", "The id of this instance") { IsRequired = true };
        var listenOption = new Option<IPEndPoint>("--listen", "The listen address") { IsRequired = true };
        var dataDirOption = new Option<DirectoryInfo>("--data-dir", "Data directory") { IsRequired = true };
        var raftListenOption = new Option<IPEndPoint>("--raft-listen", "The raft listen address") { IsRequired = true };
        var raftDataDirOption = new Option<DirectoryInfo>("--raft-data-dir", "Raft data directory") { IsRequired = true };
        var initialMembersOption = new Option<List<string>>("--initial-members",
            "Initial members list. eg. `1=127.0.0.1:15000`") { IsRequired = true, AllowMultipleArgumentsPerToken = true };

        rootCommand.AddOption(myIdOption);
        rootCommand.AddOption(listenOption);
        rootCommand.AddOption(dataDirOption);
        rootCommand.AddOption(raftListenOption);
        rootCommand.AddOption(raftDataDirOption);
        rootCommand.AddOption(initialMembersOption);

        rootCommand.SetHandler(
            (myId, dataDirInfo, listen, raftDataDirInfo, raftListen, initialMembers) => {
                var config = new LaunchConfig {
                    MyId = myId,
                    DataDir = dataDirInfo.FullName,
                    Listen = listen,
                    RaftListen = raftListen,
                    RaftDataDir = raftDataDirInfo.FullName,
                    AddressBook = new AddressBook(initialMembers)
                };
                var wait = new SemaphoreSlim(0, 1);
                var launcher = new Launcher(config);
                Console.CancelKeyPress += (_, _) => {
                    launcher.Stop();
                    launcher.Dispose();
                    wait.Release(1);
                };
                launcher.Start();
                wait.Wait();
            },
            myIdOption, dataDirOption, listenOption,
            raftDataDirOption, raftListenOption, initialMembersOption);
        await rootCommand.InvokeAsync(args);
    }
}
