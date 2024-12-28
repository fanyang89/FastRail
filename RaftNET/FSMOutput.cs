namespace RaftNET;

public class FSMOutput {
    public bool StateChanged { get; set; } = false;
    public IList<LogEntry> LogEntries { get; set; } = new List<LogEntry>();
    public IList<LogEntry> Committed { get; set; } = new List<LogEntry>();
    public IList<ToMessage> Messages = new List<ToMessage>();
    public TermVote? TermAndVote { get; set; }
    public bool AbortLeadershipTransfer { get; set; }
    public ISet<ConfigMember>? Configuration { get; set; }
    public IList<ulong> SnapshotsToDrop { get; set; } = new List<ulong>();

    public AppliedSnapshot? Snapshot { get; set; }
}