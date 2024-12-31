using System.Net;
using FastRail.Server;
using Microsoft.Extensions.Logging;
using org.apache.zookeeper;
using RaftNET.Services;

namespace FastRail.Tests;

[TestFixture]
[TestOf(typeof(RailServer))]
public class RailServerTest : RailTestBase {
    private ILogger<RailServerTest> _logger;

    [SetUp]
    public new void Setup() {
        _logger = LoggerFactory.CreateLogger<RailServerTest>();
    }

    [Test]
    public void TestRailServerCanStartAndStop() {
        var tmpDir = Directory.CreateTempSubdirectory();
        var config = new RailServer.Config {
            DataDir = tmpDir.FullName,
            EndPoint = new IPEndPoint(IPAddress.Loopback, 15000)
        };
        var server = new RailServer(config, LoggerFactory);
        server.Start();

        ulong myId = 1;
        var raftPort = 15000 + (int)myId;
        var addressBook = new AddressBook();
        addressBook.Add(myId, raftPort);
        var tmpDir2 = Directory.CreateTempSubdirectory();
        var raftConfig = new RaftService.Config {
            MyId = myId,
            DataDir = tmpDir2.FullName,
            LoggerFactory = LoggerFactory,
            StateMachine = server,
            AddressBook = addressBook,
            Listen = new IPEndPoint(IPAddress.Loopback, raftPort)
        };
        var raftServer = new RaftServer(raftConfig);
        server.SetRaftServer(raftServer);
        raftServer.Start();

        raftServer.Stop();
        server.Stop();
    }
}

public class LogWatcher(ILogger<LogWatcher> logger) : Watcher {
    public override Task process(WatchedEvent @event) {
        logger.LogInformation("Process event={}", @event);
        return Task.CompletedTask;
    }
}

[TestFixture]
[TestOf(typeof(RailServer))]
public class RailServerTest2 : RailTestBase {
    private RailServer _server;
    private const int Port = 15000;
    private ILogger<RailServerTest> _logger;

    [SetUp]
    public new void Setup() {
        _logger = LoggerFactory.CreateLogger<RailServerTest>();
        var tmpDir = Directory.CreateTempSubdirectory();
        var config = new RailServer.Config {
            DataDir = tmpDir.FullName,
            EndPoint = new IPEndPoint(IPAddress.Loopback, Port)
        };
        _server = new RailServer(config, LoggerFactory);
        _server.Start();
    }

    [TearDown]
    public new void TearDown() {
        _server.Stop();
        _server.Dispose();
    }

    [Test]
    public void TestRailServerCanAcceptConnections() {
        var client = new ZooKeeper($"127.0.0.1:{Port}", 4000, new LogWatcher(LoggerFactory.CreateLogger<LogWatcher>()));
        Thread.Sleep(5000);
    }
}