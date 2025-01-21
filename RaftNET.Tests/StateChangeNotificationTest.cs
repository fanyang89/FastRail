using RaftNET.FailureDetectors;

namespace RaftNET.Tests;

public class StateChangeNotificationTest : FSMTestBase {
    [Test]
    public void TestStateChangeNotification() {
        var fd = new DiscreteFailureDetector();
        var cfg = Messages.ConfigFromIds(Id1, Id2);
        var log = new RaftLog(new SnapshotDescriptor { Config = cfg });
        var fsm = CreateFollower(Id1, log.Clone(), fd);
        Assert.That(fsm.IsFollower, Is.True);

        ElectionTimeout(fsm);
        Assert.That(fsm.IsCandidate, Is.True);

        var output = fsm.GetOutput();
        Assert.That(output.StateChanged, Is.True);

        ElectionTimeout(fsm);
        Assert.That(fsm.IsCandidate, Is.True);

        output = fsm.GetOutput();
        Assert.Multiple(() => {
            Assert.That(output.StateChanged, Is.False);
            Assert.That(output.TermAndVote, Is.Not.Null);
        });

        fsm.Step(Id2, new VoteResponse {
            CurrentTerm = output.TermAndVote.Term, VoteGranted = true
        });
        output = fsm.GetOutput();
        Assert.Multiple(() => {
            Assert.That(output.StateChanged, Is.True);
            Assert.That(fsm.IsLeader, Is.True);
        });
    }
}
