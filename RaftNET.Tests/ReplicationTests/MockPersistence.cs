using RaftNET.Persistence;
using Serilog;

namespace RaftNET.Tests.ReplicationTests;

public class MockPersistence(
    ulong id,
    InitialState initialState,
    Snapshots snapshots,
    PersistedSnapshots persistedSnapshots
)
    : IPersistence {
    private readonly ulong _id = id;

    #region IPersistence Members

    public void StoreTermVote(ulong term, ulong vote) {
        Thread.Sleep(TimeSpan.FromMicroseconds(1));
    }

    public TermVote LoadTermVote() {
        return new TermVote { Term = initialState.Term, VotedFor = initialState.VotedFor };
    }

    public void StoreCommitIdx(ulong idx) {}

    public ulong LoadCommitIdx() {
        return 0;
    }

    public void StoreSnapshotDescriptor(SnapshotDescriptor snapshot, ulong preserveLogEntries) {
        var snp = snapshots[_id][snapshot.Id];
        persistedSnapshots[_id] = (snapshot, snp);
        Log.Information("[{my_id}] StateMachine() persist snapshot, hash={hash}", id, snp.Hasher.FinalizeUInt64());
    }

    public SnapshotDescriptor? LoadSnapshotDescriptor() {
        return initialState.Snapshot;
    }

    public void StoreLogEntries(IEnumerable<LogEntry> entries) {
        Thread.Sleep(TimeSpan.FromMicroseconds(1));
    }

    public List<LogEntry> LoadLog() {
        return initialState.Log.Select(log => log.Clone()).ToList();
    }

    public void TruncateLog(ulong idx) {}

    #endregion
}
