using RaftNET.Services;

namespace RaftNET.Tests.ReplicationTests;

public record InitialState {
    public ConfigMember Address = new();
    public ulong Term = 1;
    public ulong VotedFor = 0;
    public IList<LogEntry> Log = new List<LogEntry>();
    public SnapshotDescriptor Snapshot = new();
    public SnapshotValue SnapshotValue = new();
    public RaftServiceOptions ServerConfig = new() { AppendRequestThreshold = 200 };
}
