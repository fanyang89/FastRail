using RaftNET.FailureDetectors;

namespace RaftNET.Tests;

public class PingLeaderTest : FSMTestBase {
    [Test]
    public void TestPingLeader() {
        var fd = new DiscreteFailureDetector();
        var cfg = new Configuration {
            Current = {
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = A_ID }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = B_ID }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = C_ID }, CanVote = false },
            }
        };
        var log = new Log(new SnapshotDescriptor { Idx = 0, Config = cfg });
        var a = CreateFollower(A_ID, log.Clone(), fd);
        var b = CreateFollower(B_ID, log.Clone(), fd);
        var c = CreateFollower(C_ID, log.Clone(), fd);
        ElectionTimeout(a);
        Communicate(a, b, c);
        Assert.That(a.IsLeader, Is.True);

        fd.MarkAllDead();
        ElectionTimeout(c);
        Assert.That(c.CurrentLeader, Is.Zero);

        fd.MarkAllAlive();
        Communicate(a, b, c);
        Assert.That(c.CurrentLeader, Is.Zero);

        c.PingLeader();
        c.Tick();
        Communicate(a, b, c);
        Assert.That(c.CurrentLeader, Is.Not.Zero);
    }
}
