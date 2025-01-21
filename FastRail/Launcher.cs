using RaftNET.FailureDetectors;
using RaftNET.Persistence;
using RaftNET.Services;
using RaftNET.StateMachines;

namespace FastRail;

public class Launcher : IDisposable {
    public Launcher(LaunchConfig config) {
        Server = new Server.Server(config.ServerConfig);

        var addressBook = new AddressBook();
        var myId = config.MyId;
        var rpc = new ConnectionManager(myId, addressBook);
        var persistence = new RocksPersistence(config.DataDir);
        var options = new RaftServiceOptions();
        var clock = new SystemClock();
        var fd = new RpcFailureDetector(myId, addressBook,
            TimeSpan.FromMilliseconds(options.PingInterval),
            TimeSpan.FromMilliseconds(options.PingTimeout),
            clock);
        var service = new RaftService(config.MyId, rpc, new EmptyStateMachine(), persistence, fd, addressBook,
            new RaftServiceOptions());
        Raft = new RaftServer(service, config.Listen.Address, config.Listen.Port);
        Server.Raft = Raft;
    }

    public RaftServer Raft { get; }

    public Server.Server Server { get; }

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
