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

    public Server.Server.Config ServerConfig => new() {
        DataDir = DataDir,
        EndPoint = Listen
    };

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
    private readonly Server.Server _server;
    private readonly RaftServer _raft;

    public Launcher(LaunchConfig config) {
        _server = new Server.Server(config.ServerConfig, LoggerFactory.Instance);
        _raft = new RaftServer(config.GetRaftConfig(_server));
        _server.Raft = _raft;
    }

    public void Launch() {
        _server.Start();
        _raft.Start();
    }

    public void Stop() {
        _raft.Stop();
        _server.Stop();
    }

    public void Dispose() {
        _server.Dispose();
        GC.SuppressFinalize(this);
    }
}