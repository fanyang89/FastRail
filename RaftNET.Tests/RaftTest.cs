using Microsoft.Extensions.Logging;
using RaftNET.Services;

namespace RaftNET.Tests;

public class RaftTest {
    private ILoggerFactory _loggerFactory;
    private RaftCluster _cluster;

    [SetUp]
    public void Setup() {
        _loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
        _cluster = new RaftCluster(_loggerFactory);
        _cluster.Start();
    }

    [TearDown]
    public void TearDown() {
        _cluster.Stop();
        _loggerFactory.Dispose();
    }

    [Test]
    public void TestClusterElection() {
        Thread.Sleep(2000);
        var leader = _cluster.FindLeader();
        Assert.That(leader, Is.Not.Null);
    }
}