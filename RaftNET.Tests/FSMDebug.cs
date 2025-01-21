using System.Diagnostics;
using RaftNET.FailureDetectors;
using RaftNET.Replication;
using RaftNET.Services;

namespace RaftNET.Tests;

public class FSMDebug(
    ulong id,
    ulong currentTerm,
    ulong votedFor,
    RaftLog log,
    IFailureDetector failureDetector,
    FSM.Config config
)
    : FSM(id, currentTerm, votedFor, log, 0, failureDetector, config, Notifier) {
    private static readonly Notifier Notifier = new();

    public FollowerProgress? GetProgress(ulong id) {
        return LeaderState.Tracker.Find(id);
    }

    public bool IsLeadershipTransferActive() {
        Debug.Assert(IsLeader);
        return LeaderState.StepDown != null;
    }

    public new void BecomeFollower(ulong leader) {
        base.BecomeFollower(leader);
    }
}
