using RaftNET.Records;

namespace RaftNET;

public partial class FSM {
    public class Output {
        public IList<ToMessage> Messages = new List<ToMessage>();
        public bool AbortLeadershipTransfer { get; set; }
        public List<LogEntry> Committed { get; set; } = new();
        public ISet<ConfigMember>? Configuration { get; set; }
        public IList<LogEntry> LogEntries { get; set; } = new List<LogEntry>();
        public ulong? MaxReadIdWithQuorum { get; set; }
        public AppliedSnapshot? Snapshot { get; set; }
        public IList<ulong> SnapshotsToDrop { get; set; } = new List<ulong>();
        public bool StateChanged { get; set; }
        public TermVote? TermAndVote { get; set; }
    }
}
