using RaftNET.Concurrent;
using RaftNET.Exceptions;
using RaftNET.Replication;

namespace RaftNET;

public class Leader(int maxLogSize) {
    public readonly Tracker Tracker = new();
    public long? StepDown;
    public ulong? TimeoutNowSent;
    public LogLimiter LogLimiter = new(maxLogSize, maxLogSize);

    public void Cancel() {
        LogLimiter.Broken(new NotLeaderException());
    }
}
