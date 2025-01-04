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
    protected Launcher _launcher;
    protected Server.Server _server;
    protected RaftServer _raft;
    protected ZooKeeper _client;

    [SetUp]
    public new void Setup() {
        var addressBook = new AddressBook();
        addressBook.Add(MyId, IPAddress.Loopback, RaftPort);
        _launcher = new Launcher(new LaunchConfig {
            MyId = 1,
            DataDir = CreateTempDirectory(),
            Listen = new IPEndPoint(IPAddress.Loopback, Port),
            RaftDataDir = CreateTempDirectory(),
            RaftListen = new IPEndPoint(IPAddress.Loopback, RaftPort),
            AddressBook = addressBook
        });
        _server = _launcher.Server;
        _raft = _launcher.Raft;
        _launcher.Start();
        _client = CreateClient();
    }

    [TearDown]
    public new async Task TearDown() {
        await _client.closeAsync();
        _launcher.Stop();
        _launcher.Dispose();
    }

    protected ZooKeeper CreateClient() {
        return new ZooKeeper($"127.0.0.1:{Port}", SessionTimeout,
            new LogWatcher(LoggerFactory.CreateLogger<LogWatcher>()));
    }
}