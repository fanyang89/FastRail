using RaftNET.FailureDetectors;

namespace RaftNET.Tests;

public class ReadQuorumTest : FSMTestBase {
    [Test]
    public void TestLeaderReadQuorum() {
        var fd = new DiscreteFailureDetector();
        var cfg = new Configuration {
            Current = {
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = A_ID }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = B_ID }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = C_ID }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = D_ID }, CanVote = false },
            }
        };
        var log = new Log(new SnapshotDescriptor {
            Idx = 0, Config = cfg
        });
        var a = CreateFollower(A_ID, log.Clone(), fd);
        var b = CreateFollower(B_ID, log.Clone(), fd);
        var c = CreateFollower(C_ID, log.Clone(), fd);
        var d = CreateFollower(D_ID, log.Clone(), fd);

        ElectionTimeout(a);
        Communicate(a, b, c, d);
        Assert.That(a.IsLeader, Is.True);

        ElectionTimeout(a);
        Assert.That(a.IsLeader, Is.True);

        fd.MarkDead(C_ID);
        ElectionTimeout(a);
        Assert.That(a.IsLeader, Is.True);

        fd.MarkDead(D_ID);
        ElectionTimeout(a);
        Assert.That(a.IsLeader, Is.True);

        fd.MarkDead(B_ID);
        ElectionTimeout(a);
        Assert.That(a.IsLeader, Is.False);
    }
}
