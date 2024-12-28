using System.Diagnostics;
using Microsoft.Extensions.Logging;
using RaftNET.Services;

namespace RaftNET.Tests;

public class FSMDebug : FSM {
    private readonly static Notifier Notifier = new();

    public FSMDebug(
        ulong id, ulong currentTerm, ulong votedFor, Log log, IFailureDetector failureDetector,
        FSMConfig config, ILogger<FSM>? logger = null
    ) : base(id, currentTerm, votedFor, log, 0, failureDetector, config, Notifier, logger) {
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