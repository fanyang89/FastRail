using RaftNET.Concurrent;
using RaftNET.Replication;

namespace RaftNET;

public class Leader(int maxLogSize) {
    public readonly Tracker Tracker = new();
    public long? StepDown;
    public ulong? TimeoutNowSent;
}