using RaftNET.Services;
using Serilog;

namespace RaftNET.Tests;

public class RaftClusterTest : RaftTestBase {
    private const ulong ServerCount = 3;
    private RaftCluster _cluster;

    [SetUp]
    public new void Setup() {
        _cluster = new RaftCluster(ServerCount);
        _cluster.Start();
        Thread.Sleep((int)(FSM.ElectionTimeout * 100 * 2));
        Log.Information("Cluster started");
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
