using OneOf;

namespace RaftNET;

using Message = OneOf<
    VoteRequest, VoteResponse, AppendRequest, AppendResponse,
    InstallSnapshot, SnapshotResponse, TimeoutNowRequest>;

public record AppliedSnapshot(
    SnapshotDescriptor Snapshot,
    bool IsLocal,
    ulong PreservedLogEntries // Always 0 for non-local snapshots.
);

public class FSMOutput {
    public bool StateChanged { get; set; } = false;
    public IList<LogEntry> LogEntries { get; set; } = new List<LogEntry>();
    public IList<LogEntry> Committed { get; set; } = new List<LogEntry>();
    public IList<KeyValuePair<ulong, Message>> Messages = new List<KeyValuePair<ulong, Message>>();
    public KeyValuePair<ulong, ulong>? TermAndVote { get; set; }
    public bool AbortLeadershipTransfer { get; set; }
    public ISet<ConfigMember>? Configuration { get; set; }
    public IList<ulong> SnapshotsToDrop { get; set; } = new List<ulong>();

    public AppliedSnapshot? Snapshot { get; set; }
}