using RaftNET.Services;

namespace RaftNET.Tests.ReplicationTests;

public record InitialState {
    public ConfigMember Address = new();
    public ulong Term = 1;
    public ulong VotedFor;
    public IList<LogEntry> Log = new List<LogEntry>();
    public SnapshotDescriptor Snapshot;
    public SnapshotValue SnapshotValue;

    public RaftServiceOptions ServerConfig = new() {
        AppendRequestThreshold = 200
    };
}
