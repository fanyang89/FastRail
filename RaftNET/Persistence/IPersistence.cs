namespace RaftNET.Persistence;

public interface IPersistence {
    public ulong LoadCommitIdx();
    public List<LogEntry> LoadLog();
    public SnapshotDescriptor? LoadSnapshotDescriptor();
    public TermVote? LoadTermVote();
    public void StoreCommitIdx(ulong idx);
    public void StoreLogEntries(IEnumerable<LogEntry> entries);
    public void StoreSnapshotDescriptor(SnapshotDescriptor snapshot, ulong preserveLogEntries);
    public void StoreTermVote(ulong term, ulong vote);
    public void TruncateLog(ulong idx);
}
