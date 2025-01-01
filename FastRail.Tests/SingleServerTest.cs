using System.Net;
using Microsoft.Extensions.Logging;
using org.apache.zookeeper;
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
    private Launcher _launcher;
    private ILogger<SingleServerTest> _logger;

    private const int MyId = 1;
    private const int Port = 15000;
    private const int RaftPort = 15001;
    private const int SessionTimeout = 6000;

    [SetUp]
    public new void Setup() {
        _logger = LoggerFactory.CreateLogger<SingleServerTest>();
        var addressBook = new AddressBook();
        addressBook.Add(MyId, IPAddress.Loopback, RaftPort);
        _launcher = new Launcher(new LaunchConfig {
            MyId = 1,
            DataDir = Directory.CreateTempSubdirectory().FullName,
            Listen = new IPEndPoint(IPAddress.Loopback, Port),
            RaftDataDir = Directory.CreateTempSubdirectory().FullName,
            RaftListen = new IPEndPoint(IPAddress.Loopback, RaftPort),
            AddressBook = addressBook
        });
        _launcher.Launch();
    }

    [TearDown]
    public new void TearDown() {
        _launcher.Stop();
        _launcher.Dispose();
    }

    [Test]
    public void TestRailServerCanAcceptConnections() {
        var client = new ZooKeeper($"127.0.0.1:{Port}", SessionTimeout,
            new LogWatcher(LoggerFactory.CreateLogger<LogWatcher>()));
        Thread.Sleep(13000);
    }
}