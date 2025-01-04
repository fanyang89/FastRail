using System.Net;
using RaftNET.Services;

namespace FastRail;

public class LaunchConfig {
    public required ulong MyId;
    public required string DataDir;
    public required IPEndPoint Listen;
    public required IPEndPoint RaftListen;
    public required string RaftDataDir;
    public required AddressBook AddressBook;

    public Server.Server.Config ServerConfig => new() { DataDir = DataDir, EndPoint = Listen };

    public RaftService.Config GetRaftConfig(Server.Server server) {
        return new RaftService.Config {
            MyId = MyId,
            DataDir = RaftDataDir,
            LoggerFactory = LoggerFactory.Instance,
            StateMachine = server,
            AddressBook = AddressBook,
            Listen = RaftListen
        };
    }
}

public class Launcher : IDisposable {
    public Launcher(LaunchConfig config) {
        Server = new Server.Server(config.ServerConfig, LoggerFactory.Instance);
        Raft = new RaftServer(config.GetRaftConfig(Server));
        Server.Raft = Raft;
    }

    public Server.Server Server { get; }
    public RaftServer Raft { get; }

    public void Dispose() {
        Server.Dispose();
        GC.SuppressFinalize(this);
    }

    public void Start() {
        Server.Start();
        Raft.Start();
    }

    public void Stop() {
        Raft.Stop();
        Server.Stop();
    }
}