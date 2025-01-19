using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RaftNET.Persistence;

namespace RaftNET.Tests.ReplicationTests;

public class MockPersistence(ulong id, InitialState state, ILogger<MockPersistence>? logger = null) : IPersistence {
    private readonly PersistedSnapshots _persistedSnapshots = new();
    private readonly Snapshots _snapshots = new();
    private readonly ILogger<MockPersistence> _logger = logger ?? new NullLogger<MockPersistence>();

    public void StoreTermVote(ulong term, ulong vote) {
        Thread.Sleep(TimeSpan.FromMicroseconds(1));
    }

    public TermVote LoadTermVote() {
        return new TermVote { Term = state.Term, VotedFor = state.VotedFor };
    }

    public void StoreCommitIdx(ulong idx) {}

    public ulong LoadCommitIdx() {
        return 0;
    }

    public void StoreSnapshotDescriptor(SnapshotDescriptor snapshot, ulong preserveLogEntries) {
        var snp = _snapshots[id][snapshot.Id];
        _persistedSnapshots[id] = (snapshot, snp);
        _logger.LogInformation("[{}] StateMachine() persist snapshot, hash={}", id, snp.Hasher.FinalizeUInt64());
    }

    public SnapshotDescriptor? LoadSnapshotDescriptor() {
        return state.Snapshot;
    }

    public void StoreLogEntries(IEnumerable<LogEntry> entries) {
        Thread.Sleep(TimeSpan.FromMicroseconds(1));
    }

    public List<LogEntry> LoadLog() {
        return state.Log.Select(log => log.Clone()).ToList();
    }

    public void TruncateLog(ulong idx) {}
}
