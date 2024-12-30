using System.Diagnostics;
using Microsoft.Extensions.Logging;
using RaftNET.FailureDetectors;
using RaftNET.Replication;
using RaftNET.Services;

namespace RaftNET.Tests;

public class FSMDebug(
    ulong id,
    ulong currentTerm,
    ulong votedFor,
    Log log,
    IFailureDetector failureDetector,
    FSM.Config config,
    ILogger<FSM>? logger = null
)
    : FSM(id, currentTerm, votedFor, log, 0, failureDetector, config, Notifier, logger) {
    private readonly static Notifier Notifier = new();

    public FollowerProgress? GetProgress(ulong id) {
        return LeaderState.Tracker.Find(id);
    }

    public bool IsLeadershipTransferActive() {
        Debug.Assert(IsLeader);
        return LeaderState.StepDown != null;
    }
}