using RaftNET.FailureDetectors;

namespace RaftNET.Tests;

public class CandidateOutsideConfigurationTest : FSMTestBase {
    [Test]
    public void TestCandidateOutsideConfiguration() {
        var cfg = Messages.ConfigFromIds(A_ID, B_ID);
        var log = new RaftLog(new SnapshotDescriptor {
            Idx = 0, Config = cfg
        });
        var fd = new DiscreteFailureDetector();
        var a = CreateFollower(A_ID, log.Clone(), fd);
        var b = CreateFollower(B_ID, log.Clone(), fd);
        ElectionTimeout(a);
        Communicate(a, b);
        Assert.That(a.IsLeader, Is.True);
        var newCfg = Messages.ConfigFromIds(B_ID);
        a.AddEntry(newCfg);
        Assert.That(!b.RaftLog.GetConfiguration().IsJoint(), Is.True);
        CommunicateUntil(() => !a.GetConfiguration().IsJoint() && b.GetConfiguration().IsJoint(), a, b);
        Assert.That(a.GetConfiguration().IsJoint, Is.False);
        Assert.That(b.RaftLog.GetConfiguration().IsJoint, Is.True);
        fd.MarkDead(B_ID);
        ElectionTimeout(a);
        Assert.That(a.IsLeader, Is.False);
        fd.MarkAlive(B_ID);
        ElectionTimeout(a);
        Assert.That(a.IsCandidate, Is.True);
        CommunicateUntil(() => a.IsLeader, a, b);
        Assert.That(a.IsLeader, Is.True);
        Communicate(a, b);
        Assert.That(b.IsLeader, Is.True);
    }
}
