namespace RaftNET.Persistence;

public interface IPersistence {
    public void StoreTermVote(ulong term, ulong vote);
    public TermVote? LoadTermVote();
    public void StoreCommitIdx(ulong idx);
    public ulong LoadCommitIdx();
    public void StoreSnapshotDescriptor(SnapshotDescriptor snapshot, ulong preserveLogEntries);
    public SnapshotDescriptor? LoadSnapshotDescriptor();
    public void StoreLogEntries(IEnumerable<LogEntry> entries);
    public List<LogEntry> LoadLog();
    public void TruncateLog(ulong idx);
}
