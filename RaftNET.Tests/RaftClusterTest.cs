using Microsoft.Extensions.Logging;

namespace RaftNET.Tests;

public class RaftClusterTest {
    private ILoggerFactory _loggerFactory;
    private Services.RaftCluster _cluster;

    [SetUp]
    public void Setup() {
        _loggerFactory = LoggerFactory.Instance;
        _cluster = new Services.RaftCluster(_loggerFactory);
        _cluster.Start();
    }

    [TearDown]
    public void TearDown() {
        _cluster.Stop();
        _loggerFactory.Dispose();
    }

    [Test]
    public void TestClusterElection() {
        Thread.Sleep(5000);
        var leader = _cluster.FindLeader();
        Assert.That(leader, Is.Not.Null);
    }
}