using RaftNET.Records;

namespace RaftNET;

public partial class FSM {
    public class Output {
        public IList<ToMessage> Messages = new List<ToMessage>();
        public bool StateChanged { get; set; } = false;
        public IList<LogEntry> LogEntries { get; set; } = new List<LogEntry>();
        public List<LogEntry> Committed { get; set; } = new();
        public TermVote? TermAndVote { get; set; }
        public bool AbortLeadershipTransfer { get; set; }
        public ISet<ConfigMember>? Configuration { get; set; }
        public IList<ulong> SnapshotsToDrop { get; set; } = new List<ulong>();

        public AppliedSnapshot? Snapshot { get; set; }
    }
}