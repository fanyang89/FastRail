using RaftNET.Services;

namespace FastRail;

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
