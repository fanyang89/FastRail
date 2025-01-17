using Microsoft.Extensions.Logging;
using RaftNET.Services;

namespace RaftNET.Tests;

public class RaftClusterTest : RaftTestBase {
    private const ulong ServerCount = 3;
    private ILogger<RaftServerTest> _logger;
    private RaftCluster _cluster;

    [SetUp]
    public new void Setup() {
        _logger = LoggerFactory.CreateLogger<RaftServerTest>();
        _cluster = new RaftCluster(LoggerFactory, ServerCount);
        _cluster.Start();
        Thread.Sleep((int)(FSM.ElectionTimeout * 100 * 2));
        _logger.LogInformation("Cluster started");
    }

    [TearDown]
    public void TearDown() {
        _cluster.Stop();
    }

    [Test]
    public void TestClusterElection() {
        var leader = _cluster.FindLeader();
        Assert.That(leader, Is.Not.Null);
        var roles = _cluster.Roles();
        Assert.That(roles.Count, Is.EqualTo(ServerCount));
        Assert.That(roles.Count(x => x == Role.Leader), Is.EqualTo(1));
        Assert.That(roles.Count(x => x == Role.Follower), Is.EqualTo(2));
    }
}
