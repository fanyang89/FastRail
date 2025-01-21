using RaftNET.FailureDetectors;

namespace RaftNET.Tests;

public class VoteFromAnyStateTest : FSMTestBase {
    [Test]
    public void TestVoteFromAnyState() {
        var fd = new DiscreteFailureDetector();
        var cfg = Messages.ConfigFromIds(Id1, Id2, Id3);
        var log = new RaftLog(new SnapshotDescriptor { Config = cfg });
        var fsm = new FSMDebug(Id1, 0, 0, log, fd, FSMConfig);

        // Follower
        Assert.That(fsm.IsFollower, Is.True);
        fsm.Step(Id2, new VoteRequest { CurrentTerm = 1, LastLogIdx = 1, LastLogTerm = 1 });
        var output = fsm.GetOutput();
        Assert.That(output.Messages.Count, Is.EqualTo(1));
        Assert.That(output.Messages.Last().Message.IsVoteResponse, Is.True);
        var response = output.Messages.Last().Message.VoteResponse;
        Assert.Multiple(() => {
            Assert.That(response.VoteGranted, Is.True);
            Assert.That(response.CurrentTerm, Is.EqualTo(1));
        });

        // TODO: pre candidate when (if) implemented

        // Candidate
        Assert.That(fsm.IsCandidate, Is.False);
        ElectionTimeout(fsm);
        Assert.That(fsm.IsCandidate, Is.True);
        output = fsm.GetOutput();
        Assert.That(output.TermAndVote, Is.Not.Null);
        fsm.Step(Id2, new VoteRequest { CurrentTerm = output.TermAndVote.Term + 1, LastLogIdx = 1, LastLogTerm = 1 });
        output = fsm.GetOutput();
        Assert.That(output.Messages, Has.Count.EqualTo(1));
        Assert.That(output.Messages.Last().Message.IsVoteResponse, Is.True);
        response = output.Messages.Last().Message.VoteResponse;
        Assert.Multiple(() => {
            Assert.That(response.VoteGranted, Is.True);
            Assert.That(response.CurrentTerm, Is.EqualTo(3));
        });

        // Leader
        ElectionTimeout(fsm);
        Assert.That(fsm.IsCandidate, Is.True);
        output = fsm.GetOutput();
        Assert.That(output.TermAndVote, Is.Not.Null);
        var currentTerm = output.TermAndVote.Term;
        fsm.Step(Id2, new VoteResponse { CurrentTerm = currentTerm, VoteGranted = true });
        Assert.That(fsm.IsLeader, Is.True);
        fsm.GetOutput();
        fd.MarkAllDead();
        ElectionThreshold(fsm);
        fsm.GetOutput();
        fsm.Step(Id2, new VoteRequest { CurrentTerm = currentTerm + 2, LastLogIdx = 42, LastLogTerm = currentTerm + 1 });
        Assert.That(fsm.IsFollower, Is.True);
        output = fsm.GetOutput();
        Assert.That(output.Messages.Count, Is.EqualTo(1));
        Assert.That(output.Messages.Last().Message.IsVoteResponse, Is.True);
        response = output.Messages.Last().Message.VoteResponse;
        Assert.Multiple(() => {
            Assert.That(response.VoteGranted, Is.True);
            Assert.That(response.CurrentTerm, Is.EqualTo(6));
        });
    }
}
