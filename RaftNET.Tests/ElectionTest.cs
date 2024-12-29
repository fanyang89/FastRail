using System.Text;
using Microsoft.Extensions.Logging;
using RaftNET.FailureDetectors;

namespace RaftNET.Tests;

public class ElectionTest : FSMTestBase {
    [TestCase(true)]
    [TestCase(false)]
    public void SingleNode(bool enablePreVote) {
        var fsmConfig = new FSM.Config(
            MaxLogSize: 4 * 1024 * 1024,
            AppendRequestThreshold: 1,
            EnablePreVote: enablePreVote
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
        Assert.That(output.LogEntries.First().Dummy, Is.Not.Null);
        Assert.That(output.Committed.Count, Is.Zero);

        ElectionTimeout(fsm);
        Assert.That(fsm.IsLeader);
        output = fsm.GetOutput();
        Assert.That(output.TermAndVote, Is.Null);
        Assert.That(output.Messages.Count, Is.Zero);
        Assert.That(output.LogEntries.Count, Is.Zero);
        Assert.That(output.Committed.Count, Is.EqualTo(1));
        Assert.That(output.Committed.First().Dummy, Is.Not.Null);
    }

    [Test]
    public void SingleNodeQuiet() {
        var cfg = Messages.ConfigFromIds(Id1);
        var log = new Log(new SnapshotDescriptor { Config = cfg });
        var fsm = CreateFollower(Id1, log);

        // Immediately converts from leader to follower if quorum=1
        ElectionTimeout(fsm);
        Assert.That(fsm.IsLeader, Is.True);

        fsm.GetOutput();
        fsm.AddEntry(Encoding.UTF8.GetBytes(""));
        Assert.That(fsm.GetOutput().Messages.Count, Is.Zero);

        fsm.Tick();
        Assert.That(fsm.GetOutput().Messages.Count, Is.Zero);
    }

    [Test]
    public void TwoNodes() {
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

        fsm.Step(Id2, new VoteResponse {
            CurrentTerm = output.TermAndVote.Term,
            VoteGranted = true
        });
        Assert.That(fsm.IsLeader, Is.True);

        fsm.Step(Id2, new VoteResponse {
            CurrentTerm = output.TermAndVote.Term - 1,
            VoteGranted = false
        });
        Assert.That(fsm.IsLeader, Is.True);

        fd.MarkAllDead();
        ElectionThreshold(fsm);
        fsm.Step(Id2, new VoteRequest {
            CurrentTerm = output.TermAndVote.Term + 2
        });
        Assert.That(fsm.IsFollower, Is.True);

        ElectionTimeout(fsm);
        Assert.That(fsm.IsCandidate, Is.True);
        output = fsm.GetOutput();
        Assert.That(output.TermAndVote, Is.Not.Null);
        fsm.Step(Id2, new VoteRequest {
            CurrentTerm = output.TermAndVote.Term + 1
        });
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
    public void FourNodes() {
        var fd = new DiscreteFailureDetector();
        var cfg = Messages.ConfigFromIds(Id1, Id2, Id3, Id4);
        var log = new Log(new SnapshotDescriptor { Config = cfg });
        var fsm = CreateFollower(Id1, log, fd);
        Assert.That(fsm.IsFollower);

        fsm.Step(Id4, new AppendRequest {
            CurrentTerm = 1, PrevLogIdx = 1, PrevLogTerm = 1
        });

        fsm.GetOutput();

        fsm.Step(Id3, new VoteRequest {
            CurrentTerm = 1, LastLogIdx = 1, LastLogTerm = 1
        });

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
}