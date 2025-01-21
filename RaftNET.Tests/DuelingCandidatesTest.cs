using RaftNET.FailureDetectors;

namespace RaftNET.Tests;

public class DuelingCandidatesTest : FSMTestBase {
    [Test]
    public void TestDuelingCandidates() {
        var cfg = Messages.ConfigFromIds(Id1, Id2, Id3);
        var log1 = new RaftLog(new SnapshotDescriptor { Config = cfg });
        var fsm1 = new FSMDebug(Id1, 0, 0, log1, new TrivialFailureDetector(), FSMConfig);
        var log2 = new RaftLog(new SnapshotDescriptor { Config = cfg });
        var fsm2 = new FSMDebug(Id2, 0, 0, log2, new TrivialFailureDetector(), FSMConfig);
        var log3 = new RaftLog(new SnapshotDescriptor { Config = cfg });
        var fsm3 = new FSMDebug(Id3, 0, 0, log3, new TrivialFailureDetector(), FSMConfig);

        MakeCandidate(fsm1);
        MakeCandidate(fsm3);
        Communicate(fsm1, fsm2);
        Assert.Multiple(() => {
            Assert.That(fsm1.IsLeader, Is.True);
            Assert.That(fsm3.IsCandidate, Is.True);
        });
        Communicate(fsm3, fsm2);
        Assert.That(fsm3.IsCandidate, Is.True);

        ElectionTimeout(fsm3);
        Communicate(fsm3, fsm1, fsm2);
        Assert.Multiple(() => {
            Assert.That(fsm1.LogLastIdx, Is.EqualTo(1));
            Assert.That(fsm2.LogLastIdx, Is.EqualTo(1));
            Assert.That(fsm3.LogLastIdx, Is.EqualTo(0));
            Assert.That(fsm1.LogLastTerm, Is.EqualTo(1));
            Assert.That(fsm2.LogLastTerm, Is.EqualTo(1));
            Assert.That(fsm3.LogLastTerm, Is.EqualTo(0));
        });
    }

    [Test]
    public void TestDuelingPreCandidates() {
        var cfg = Messages.ConfigFromIds(Id1, Id2, Id3);
        var log1 = new RaftLog(new SnapshotDescriptor { Config = cfg });
        var fsm1 = new FSMDebug(Id1, 0, 0, log1, new TrivialFailureDetector(), FSMPreVoteConfig);
        var log2 = new RaftLog(new SnapshotDescriptor { Config = cfg });
        var fsm2 = new FSMDebug(Id2, 0, 0, log2, new TrivialFailureDetector(), FSMPreVoteConfig);
        var log3 = new RaftLog(new SnapshotDescriptor { Config = cfg });
        var fsm3 = new FSMDebug(Id3, 0, 0, log3, new TrivialFailureDetector(), FSMPreVoteConfig);

        MakeCandidate(fsm1);
        MakeCandidate(fsm3);

        Communicate(fsm1, fsm2);
        Assert.Multiple(() => {
            Assert.That(fsm1.IsLeader, Is.True);
            Assert.That(fsm3.IsCandidate, Is.True);
        });
        Communicate(fsm3, fsm2);
        Assert.That(fsm3.IsFollower, Is.True);

        ElectionTimeout(fsm3);
        Assert.That(fsm3.IsCandidate, Is.True);
        Communicate(fsm3, fsm1, fsm2);
        Assert.Multiple(() => {
            Assert.That(fsm1.LogLastIdx, Is.EqualTo(1));
            Assert.That(fsm2.LogLastIdx, Is.EqualTo(1));
            Assert.That(fsm3.LogLastIdx, Is.EqualTo(0));
            Assert.That(fsm1.LogLastTerm, Is.EqualTo(1));
            Assert.That(fsm2.LogLastTerm, Is.EqualTo(1));
            Assert.That(fsm3.LogLastTerm, Is.EqualTo(0));
            Assert.That(fsm1.CurrentTerm, Is.EqualTo(1));
            Assert.That(fsm2.CurrentTerm, Is.EqualTo(1));
            Assert.That(fsm3.CurrentTerm, Is.EqualTo(1));
        });

    }
}
