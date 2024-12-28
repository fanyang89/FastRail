using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestPlatform.Common.Utilities;

namespace RaftNET.Tests;

public class FSMDebug : FSM {
    public FSMDebug(
        ulong id, ulong currentTerm, ulong votedFor, Log log, IFailureDetector failureDetector,
        FSMConfig config, ILogger<FSM>? logger = null
    ) : base(id, currentTerm, votedFor, log, 0, failureDetector, config, logger) {
    }

    public void BecomeFollower(ulong leader) {
        base.BecomeFollower(leader);
    }

    public FollowerProgress? GetProgress(ulong id) {
        return LeaderState.Tracker.Find(id);
    }

    public Log GetLog() {
        return base.GetLog();
    }

    public bool IsLeadershipTransferActive() {
        Debug.Assert(IsLeader);
        return LeaderState.stepDown != null;
    }
}

public class FSMTestBase {
    protected void ElectionTimeout(FSM fsm) {
        for (var i = 0; i <= 2 * FSM.ElectionTimeout; ++i) {
            fsm.Tick();
        }
    }

    protected void ElectionThreshold(FSM fsm) {
        for (var i = 0; i < FSM.ElectionTimeout; ++i) {
            fsm.Tick();
        }
    }

    protected void MakeCandidate(FSM fsm) {
        Debug.Assert(fsm.IsFollower);
        while (fsm.IsFollower) {
            fsm.Tick();
        }
    }
}

public class ElectionTest : FSMTestBase {
    private ulong Id1 = 1;

    [TestCase(true)]
    [TestCase(false)]
    public void SingleNodeElection(bool enablePreVote) {
        var fsmConfig = new FSMConfig {
            EnablePreVote = enablePreVote
        };
        var cfg = Messages.ConfigFromIds(Id1);
        var log = new Log(new SnapshotDescriptor { Config = cfg });
        var fsm = new FSMDebug(Id1, 0, 0, log, new TrivialFailureDetector(), fsmConfig);

        ElectionTimeout(fsm);
        Assert.That(fsm.IsLeader, Is.True);

        var output = fsm.GetOutput();
        Assert.That(output.TermAndVote, Is.Not.Null);
        Assert.That(output.TermAndVote.Value.Key, Is.Not.Zero);
        Assert.That(output.TermAndVote.Value.Value, Is.Not.Zero);
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
}