using Microsoft.Extensions.Logging;
using RaftNET.FailureDetectors;

namespace RaftNET.Tests;

public class ElectionTest : FSMTestBase {
    [TestCase(true)]
    [TestCase(false)]
    public void TestSingleNodeElection(bool enablePreVote) {
        var fsmConfig = new FSM.Config(
            maxLogSize: 4 * 1024 * 1024,
            appendRequestThreshold: 1,
            enablePreVote: enablePreVote
        );
        var cfg = Messages.ConfigFromIds(Id1);
        var log = new Log(new SnapshotDescriptor { Config = cfg });
        var fsm = new FSMDebug(Id1, 0, 0, log, new TrivialFailureDetector(), fsmConfig, LoggerFactory.CreateLogger<FSM>());

        ElectionTimeout(fsm);
        Assert.That(fsm.IsLeader, Is.True);

        var output = fsm.GetOutput();
        Assert.That(output.TermAndVote, Is.Not.Null);
        Assert.That(output.TermAndVote.Term, Is.Not.Zero);
        Assert.That(output.TermAndVote.VotedFor, Is.Not.Zero);
        Assert.That(output.Messages.Count, Is.Zero);
        Assert.That(output.LogEntries.Count, Is.EqualTo(1));
        Assert.That(output.LogEntries.First().Fake, Is.Not.Null);
        Assert.That(output.Committed.Count, Is.Zero);

        ElectionTimeout(fsm);
        Assert.That(fsm.IsLeader);
        output = fsm.GetOutput();
        Assert.That(output.TermAndVote, Is.Null);
        Assert.That(output.Messages.Count, Is.Zero);
        Assert.That(output.LogEntries.Count, Is.Zero);
        Assert.That(output.Committed.Count, Is.EqualTo(1));
        Assert.That(output.Committed.First().Fake, Is.Not.Null);
    }

    [Test]
    public void TestTwoNodesElection() {
        var fd = new DiscreteFailureDetector();
        var cfg = Messages.ConfigFromIds(Id1, Id2);
        var log = new Log(new SnapshotDescriptor { Config = cfg });
        var fsm = CreateFollower(Id1, log, fd);
        // Initial state is follower
        Assert.That(fsm.IsFollower);
        // After election timeout, a follower becomes a candidate
        ElectionTimeout(fsm);
        Assert.That(fsm.IsCandidate);
        // If nothing happens, the candidate stays this way
        ElectionTimeout(fsm);
        Assert.That(fsm.IsCandidate);

        var output = fsm.GetOutput();
        Assert.That(output.TermAndVote, Is.Not.Null);

        fsm.Step(Id2, new VoteResponse { CurrentTerm = output.TermAndVote.Term, VoteGranted = true });
        Assert.That(fsm.IsLeader, Is.True);

        fsm.Step(Id2, new VoteResponse { CurrentTerm = output.TermAndVote.Term - 1, VoteGranted = false });
        Assert.That(fsm.IsLeader, Is.True);

        fd.MarkAllDead();
        ElectionThreshold(fsm);
        fsm.Step(Id2, new VoteRequest { CurrentTerm = output.TermAndVote.Term + 2 });
        Assert.That(fsm.IsFollower, Is.True);

        ElectionTimeout(fsm);
        Assert.That(fsm.IsCandidate, Is.True);
        output = fsm.GetOutput();
        Assert.That(output.TermAndVote, Is.Not.Null);
        fsm.Step(Id2, new VoteRequest { CurrentTerm = output.TermAndVote.Term + 1 });
        Assert.That(fsm.IsFollower, Is.True);

        fsm.GetOutput();
        MakeCandidate(fsm);
        Assert.That(fsm.IsCandidate, Is.True);

        output = fsm.GetOutput();
        var msg = output.Messages.Last().Message.VoteRequest;
        fsm.Step(Id2, msg);

        // We could figure out this round is going to a nowhere, but
        // we're not that smart and simply wait for a vote_reply.
        Assert.That(fsm.IsCandidate, Is.True);
        output = fsm.GetOutput();
        var rsp = output.Messages.Last().Message.VoteResponse;
        Assert.That(rsp.VoteGranted, Is.False);
    }

    [Test]
    public void TestTwoNodesPreVote() {
        var cfg = Messages.ConfigFromIds(Id1, Id2);
        var log = new Log(new SnapshotDescriptor { Config = cfg });
        var fsm = new FSMDebug(Id1, 0, 0, log, new TrivialFailureDetector(), FSMPreVoteConfig,
            LoggerFactory.CreateLogger<FSM>());
        Assert.That(fsm.IsFollower, Is.True);

        ElectionTimeout(fsm);
        Assert.Multiple(() => {
            Assert.That(fsm.IsPreVoteCandidate, Is.True);
            Assert.That(fsm.CurrentTerm, Is.Zero);
        });

        // If nothing happens, the candidate stays this way
        ElectionTimeout(fsm);
        Assert.Multiple(() => {
            Assert.That(fsm.IsPreVoteCandidate, Is.True);
            Assert.That(fsm.CurrentTerm, Is.Zero);
        });

        var output = fsm.GetOutput();
        Assert.That(output.TermAndVote, Is.Null);
        fsm.Step(Id2, new VoteResponse {
            CurrentTerm = fsm.CurrentTerm,
            VoteGranted = true,
            IsPreVote = true
        });
        Assert.Multiple(() => {
            Assert.That(fsm.IsCandidate, Is.True);
            Assert.That(fsm.IsPreVoteCandidate, Is.False);
            Assert.That(fsm.CurrentTerm, Is.EqualTo(1));
        });

        ElectionTimeout(fsm);
        fsm.Step(Id2, new VoteResponse {
            CurrentTerm = 2,
            VoteGranted = false,
            IsPreVote = true
        });
        Assert.Multiple(() => {
            Assert.That(fsm.IsFollower, Is.True);
            Assert.That(fsm.CurrentTerm, Is.EqualTo(2));
        });

        ElectionTimeout(fsm);
        fsm.GetOutput();

        fsm.Step(Id2, new VoteRequest {
            CurrentTerm = 1, LastLogIdx = 0, LastLogTerm = 0,
            IsPreVote = true, Force = false
        });
        output = fsm.GetOutput();
        Assert.Multiple(() => {
            Assert.That(output.Messages, Has.Count.EqualTo(1));
            Assert.That(output.Messages.Last().Message.IsVoteResponse, Is.True);
            var msg = output.Messages.Last().Message.VoteResponse;
            Assert.That(msg.CurrentTerm, Is.EqualTo(2));
            Assert.That(msg.VoteGranted, Is.False);
        });

        fsm.Step(Id2, new VoteResponse { CurrentTerm = 3, VoteGranted = false, IsPreVote = true });
        Assert.That(fsm.IsFollower, Is.True);
        fsm.Step(Id2, new VoteRequest {
            CurrentTerm = 4, LastLogIdx = 0, LastLogTerm = 0,
            IsPreVote = true, Force = false
        });
        output = fsm.GetOutput();
        Assert.Multiple(() => {
            Assert.That(output.Messages, Has.Count.EqualTo(1));
            Assert.That(output.Messages.Last().Message.IsVoteResponse, Is.True);
            var msg = output.Messages.Last().Message.VoteResponse;
            Assert.That(msg.CurrentTerm, Is.EqualTo(4));
            Assert.That(msg.VoteGranted, Is.True);
            Assert.That(fsm.CurrentTerm, Is.EqualTo(3));
        });
    }

    [Test]
    public void TestFourNodesElection() {
        var fd = new DiscreteFailureDetector();
        var cfg = Messages.ConfigFromIds(Id1, Id2, Id3, Id4);
        var log = new Log(new SnapshotDescriptor { Config = cfg });
        var fsm = CreateFollower(Id1, log, fd);
        Assert.That(fsm.IsFollower);

        fsm.Step(Id4, new AppendRequest { CurrentTerm = 1, PrevLogIdx = 1, PrevLogTerm = 1 });

        fsm.GetOutput();

        fsm.Step(Id3, new VoteRequest { CurrentTerm = 1, LastLogIdx = 1, LastLogTerm = 1 });

        var output = fsm.GetOutput();
        var reply = output.Messages.Last().Message.VoteResponse;
        Assert.That(!reply.VoteGranted);

        fd.MarkAllDead();
        ElectionTimeout(fsm);
        Assert.That(fsm.IsCandidate);

        output = fsm.GetOutput();
        Assert.That(output.TermAndVote, Is.Not.Null);
        var currentTerm = output.TermAndVote.Term;
        fsm.Step(Id2, new VoteResponse { CurrentTerm = currentTerm, VoteGranted = true });
        Assert.That(fsm.IsCandidate);
        fsm.Step(Id3, new VoteResponse { CurrentTerm = currentTerm, VoteGranted = true });
        Assert.That(fsm.IsLeader);
    }

    [Test]
    public void TestFourNodesPreVote() {
        var fd = new DiscreteFailureDetector();
        var cfg = Messages.ConfigFromIds(Id1, Id2, Id3, Id4);
        var log = new Log(new SnapshotDescriptor { Config = cfg });
        var fsm = new FSMDebug(Id1, 0, 0, log, fd, FSMPreVoteConfig);

        Assert.That(fsm.IsFollower, Is.True);

        fsm.Step(Id4, new AppendRequest {
            CurrentTerm = 1, PrevLogIdx = 1, PrevLogTerm = 1
        });

        fsm.GetOutput();

        fsm.Step(Id3, new VoteRequest {
            CurrentTerm = 1,
            LastLogIdx = 1,
            LastLogTerm = 1,
            IsPreVote = true,
            Force = false
        });

        var output = fsm.GetOutput();
        Assert.That(output.Messages.Last().Message.IsVoteResponse, Is.True);
        var reply = output.Messages.Last().Message.VoteResponse;
        Assert.Multiple(() => {
            Assert.That(reply.VoteGranted, Is.False);
            Assert.That(reply.IsPreVote, Is.True);
        });

        fd.MarkAllDead();
        ElectionTimeout(fsm);
        Assert.Multiple(() => {
            Assert.That(fsm.IsCandidate, Is.True);
            Assert.That(fsm.IsPreVoteCandidate, Is.True);
        });

        output = fsm.GetOutput();
        Assert.That(output.TermAndVote, Is.Null);
        fsm.Step(Id2, new VoteResponse {
            CurrentTerm = fsm.CurrentTerm + 1,
            VoteGranted = true,
            IsPreVote = true
        });
        Assert.Multiple(() => {
            Assert.That(fsm.IsCandidate, Is.True);
            Assert.That(fsm.IsPreVoteCandidate, Is.True);
        });

        fsm.Step(Id3, new VoteResponse {
            CurrentTerm = fsm.CurrentTerm + 1,
            VoteGranted = true,
            IsPreVote = true
        });
        Assert.Multiple(() => {
            Assert.That(fsm.IsCandidate, Is.True);
            Assert.That(fsm.IsPreVoteCandidate, Is.False);
        });

        fsm.Step(Id2, new VoteRequest {
            CurrentTerm = fsm.CurrentTerm + 1,
            LastLogIdx = 1,
            LastLogTerm = 1,
            IsPreVote = false,
            Force = false
        });

        fsm.GetOutput();

        fsm.Step(Id3, new VoteRequest {
            CurrentTerm = fsm.CurrentTerm + 1,
            LastLogIdx = 1,
            LastLogTerm = 1,
            IsPreVote = true,
            Force = false
        });

        output = fsm.GetOutput();

        Assert.Multiple(() => {
            Assert.That(output.Messages, Has.Count.EqualTo(1));
            Assert.That(output.Messages.Last().Message.IsVoteResponse, Is.True);
        });
        reply = output.Messages.Last().Message.VoteResponse;
        Assert.Multiple(() => {
            Assert.That(reply.VoteGranted, Is.True);
            Assert.That(reply.IsPreVote, Is.True);
        });
    }
}
