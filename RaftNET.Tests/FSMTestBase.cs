using Microsoft.Extensions.Logging;
using RaftNET.FailureDetectors;

namespace RaftNET.Tests;

public class FSMTestBase : RaftTestBase {
    protected const ulong Id1 = 1;
    protected const ulong Id2 = 2;
    protected const ulong Id3 = 3;
    protected const ulong Id4 = 4;
    protected const ulong Id5 = 5;

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

    protected readonly FSM.Config FSMConfig = new(
        MaxLogSize: 4 * 1024 * 1024,
        AppendRequestThreshold: 1,
        EnablePreVote: false
    );

    protected readonly FSM.Config FSMPreVoteConfig = new(
        MaxLogSize: 4 * 1024 * 1024,
        AppendRequestThreshold: 1,
        EnablePreVote: true
    );

    protected FSMDebug CreateFollower(ulong id, Log log, IFailureDetector fd) {
        return new FSMDebug(id, 0, 0, log, fd, FSMConfig, LoggerFactory.CreateLogger<FSM>());
    }

    protected FSMDebug CreateFollower(ulong id, Log log) {
        return CreateFollower(id, log, new TrivialFailureDetector());
    }
}