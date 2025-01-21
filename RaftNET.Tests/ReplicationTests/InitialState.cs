using RaftNET.Services;

namespace RaftNET.Tests.ReplicationTests;

public record InitialState {
    public ConfigMember Address = new();
    public IList<LogEntry> Log = new List<LogEntry>();
    public RaftServiceOptions ServerConfig = new() { AppendRequestThreshold = 200 };
    public SnapshotDescriptor Snapshot = new();
    public SnapshotValue SnapshotValue = new();
    public ulong Term = 1;
    public ulong VotedFor = 0;
}
