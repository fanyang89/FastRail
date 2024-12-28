namespace RaftNET.Tests;

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
        Assert.That(fsm.IsFollower, Is.True);
        while (fsm.IsFollower) {
            fsm.Tick();
        }
    }

    protected readonly FSMConfig FSMConfig = new FSMConfig {
        AppendRequestThreshold = 1,
        EnablePreVote = false
    };

    protected readonly FSMConfig FSMPreVoteConfig = new FSMConfig {
        AppendRequestThreshold = 1,
        EnablePreVote = true
    };

    protected FSMDebug CreateFollower(ulong id, Log log, IFailureDetector fd) {
        return new FSMDebug(id, 0, 0, log, fd, FSMConfig);
    }

    protected FSMDebug CreateFollower(ulong id, Log log) {
        return CreateFollower(id, log, new TrivialFailureDetector());
    }
}