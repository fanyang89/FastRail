using System.Net;
using FastRail.Server;
using Microsoft.Extensions.Logging;
using org.apache.zookeeper;
using org.apache.zookeeper.data;
using RaftNET.Services;

namespace FastRail.Tests;

public class LogWatcher(ILogger<LogWatcher> logger) : Watcher {
    public override Task process(WatchedEvent @event) {
        logger.LogInformation("Process event={}", @event);
        return Task.CompletedTask;
    }
}

[TestFixture]
[TestOf(typeof(Server.Server))]
public class SingleServerTest : TestBase {
    private ILogger<SingleServerTest> _logger;
    private Launcher _launcher;
    private Server.Server _server;
    private RaftServer _raft;

    private const int MyId = 1;
    private const int Port = 15000;
    private const int RaftPort = 15001;
    private const int SessionTimeout = 6000;

    private string CreateTempDirectory() {
        var dir = Directory.CreateTempSubdirectory().FullName;
        _logger.LogInformation("Create temp directory: {}", dir);
        return dir;
    }

    [SetUp]
    public new void Setup() {
        _logger = LoggerFactory.CreateLogger<SingleServerTest>();
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
        _launcher.Launch();
    }

    [TearDown]
    public new void TearDown() {
        _launcher.Stop();
        _launcher.Dispose();
    }

    private ZooKeeper CreateClient() {
        return new ZooKeeper($"127.0.0.1:{Port}", SessionTimeout, new LogWatcher(LoggerFactory.CreateLogger<LogWatcher>()));
    }

    [Test]
    public void TestSingleServerCanAcceptConnections() {
        _ = CreateClient();
        Thread.Sleep(4000);
        Assert.That(_server.PingCount, Is.GreaterThan(0));
    }

    [Test]
    public async Task TestSingleServerCanCreate() {
        var client = CreateClient();
        const string path = "/test-node1";
        const string expected = "test-value";
        var realPath = await client.createAsync(path, expected.ToBytes(),
            [new ACL((int)ZooDefs.Perms.ALL, ZooDefs.Ids.ANYONE_ID_UNSAFE)],
            CreateMode.PERSISTENT);
        Assert.That(realPath, Is.EqualTo(path));
        var stat = await client.existsAsync(path);
        Assert.That(stat, Is.Not.Null);
    }

    [Test]
    public async Task TestSingleServerCanSetAndGet() {
        var client = CreateClient();
        const string path = "/test-node2";
        const string expected = "test-value";
        var realPath = await client.createAsync(path, expected.ToBytes(),
            [new ACL((int)ZooDefs.Perms.ALL, ZooDefs.Ids.ANYONE_ID_UNSAFE)],
            CreateMode.PERSISTENT);
        Assert.That(realPath, Is.EqualTo(path));
        var stat = await client.existsAsync(path);
        Assert.That(stat, Is.Not.Null);
        var result = await client.getDataAsync(path);
        Assert.That(result.Data, Is.EqualTo(expected.ToBytes()));
    }
}