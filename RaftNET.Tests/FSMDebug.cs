using System.Diagnostics;
using Microsoft.Extensions.Logging;
using RaftNET.FailureDetectors;
using RaftNET.Replication;
using RaftNET.Services;

namespace RaftNET.Tests;

public class FSMDebug : FSM {
    private readonly static Notifier Notifier = new();

    public FSMDebug(
        ulong id, ulong currentTerm, ulong votedFor, Log log, IFailureDetector failureDetector,
        Config config, ILogger<FSM>? logger = null
    ) : base(id, currentTerm, votedFor, log, 0, failureDetector, config, Notifier, logger) {
    }

    public FollowerProgress? GetProgress(ulong id) {
        return LeaderState.Tracker.Find(id);
    }

    public bool IsLeadershipTransferActive() {
        Debug.Assert(IsLeader);
        return LeaderState.StepDown != null;
    }
}