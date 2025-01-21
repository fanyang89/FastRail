using RaftNET.Concurrent;
using RaftNET.Exceptions;
using RaftNET.Replication;

namespace RaftNET;

public class Leader(int maxLogSize) {
    public readonly LogLimiter LogLimiter = new(maxLogSize, maxLogSize);
    public readonly Tracker Tracker = new();
    public ulong LastReadId = 0;
    public bool LastReadIdChanged = false;
    public ulong MaxReadIdWithQuorum = 0;
    public long? StepDown;
    public ulong? TimeoutNowSent;

    public void Cancel() {
        LogLimiter.Broken(new NotLeaderException());
    }
}
