using RaftNET.Concurrent;
using RaftNET.Exceptions;
using RaftNET.Replication;

namespace RaftNET;

public class Leader(int maxLogSize) {
    public readonly Tracker Tracker = new();
    public long? StepDown;
    public ulong? TimeoutNowSent;
    public readonly LogLimiter LogLimiter = new(maxLogSize, maxLogSize);
    public ulong LastReadId = 0;
    public bool LastReadIdChanged = false;
    public ulong MaxReadIdWithQuorum = 0;

    public void Cancel() {
        LogLimiter.Broken(new NotLeaderException());
    }
}
