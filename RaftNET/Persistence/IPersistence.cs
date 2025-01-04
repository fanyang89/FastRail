namespace RaftNET.Persistence;

public interface IPersistence {
    void StoreTermVote(ulong term, ulong vote);
    TermVote? LoadTermVote();
    void StoreCommitIdx(ulong idx);
    ulong LoadCommitIdx();
    void StoreSnapshotDescriptor(SnapshotDescriptor snapshot, ulong preserveLogEntries);
    SnapshotDescriptor? LoadSnapshotDescriptor();
    void StoreLogEntries(IEnumerable<LogEntry> entries);
    List<LogEntry> LoadLog();
    void TruncateLog(ulong idx);
}