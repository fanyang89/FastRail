using RaftNET.FailureDetectors;

namespace RaftNET.Tests;

public class ProgressPauseTest : FSMTestBase {
    [Test]
    public void TestProgressPause() {
        var cfg = Messages.ConfigFromIds(Id1, Id2);
        var log = new RaftLog(new SnapshotDescriptor { Config = cfg });

        var fsm = new FSMDebug(Id1, 0, 0, log, new TrivialFailureDetector(), FSMConfig);

        ElectionTimeout(fsm);
        var output = fsm.GetOutput();
        Assert.That(output.TermAndVote, Is.Not.Null);
        var currentTerm = output.TermAndVote.Term;
        fsm.Step(Id2, new VoteResponse {
            CurrentTerm = currentTerm,
            VoteGranted = true
        });
        Assert.That(fsm.IsLeader, Is.True);

        fsm.AddEntry("1");
        fsm.AddEntry("2");
        fsm.AddEntry("3");
        output = fsm.GetOutput();
        Assert.That(output.Messages, Has.Count.EqualTo(1));
    }
}
