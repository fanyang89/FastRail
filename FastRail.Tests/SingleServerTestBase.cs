using System.Net;
using Microsoft.Extensions.Logging;
using org.apache.zookeeper;
using RaftNET.Services;

namespace FastRail.Tests;

public class SingleServerTestBase : TestBase {
    protected const int MyId = 1;
    protected const int Port = 15000;
    protected const int RaftPort = 15001;
    protected const int SessionTimeout = 6000;
    protected Launcher Launcher;
    protected Server.Server Server;
    protected RaftServer Raft;
    protected ZooKeeper Client;

    [SetUp]
    public new void Setup() {
        var addressBook = new AddressBook();
        addressBook.Add(MyId, IPAddress.Loopback, RaftPort);
        Launcher = new Launcher(new LaunchConfig {
            MyId = 1,
            DataDir = CreateTempDirectory(),
            Listen = new IPEndPoint(IPAddress.Loopback, Port),
            RaftDataDir = CreateTempDirectory(),
            RaftListen = new IPEndPoint(IPAddress.Loopback, RaftPort),
            AddressBook = addressBook
        });
        Server = Launcher.Server;
        Raft = Launcher.Raft;
        Launcher.Start();
        Client = CreateClient();
    }

    [TearDown]
    public new async Task TearDown() {
        await Client.closeAsync();
        Launcher.Stop();
        Launcher.Dispose();
    }

    protected ZooKeeper CreateClient() {
        return new ZooKeeper($"127.0.0.1:{Port}", SessionTimeout,
            new LogWatcher(LoggerFactory.CreateLogger<LogWatcher>()));
    }
}
