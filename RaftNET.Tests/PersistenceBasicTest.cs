namespace RaftNET.Tests;

public class PersistenceBasicTest {
    [Test]
    public void Basic() {
        var dataDir = Directory.CreateTempSubdirectory();
        ulong term = 1;
        ulong votedFor = 1;
        ulong commitIdx = 1;
        var entries = new List<LogEntry>();
        {
            using var p = new RocksPersistence(dataDir.FullName);
            Assert.That(p.LoadTermVote(), Is.Null);

            p.StoreTermVote(term, votedFor);
            p.StoreCommitIdx(commitIdx);

            var snapshot = new SnapshotDescriptor { Idx = 1 };
            p.StoreSnapshotDescriptor(snapshot, 0);

            for (ulong i = 2; i < 12; ++i) {
                entries.Add(new LogEntry {
                    Idx = i,
                    Term = i + 100
                });
            }
            p.StoreLogEntries(entries);
        }
        {
            using var p = new RocksPersistence(dataDir.FullName);
            var termVote = p.LoadTermVote();
            Assert.That(termVote, Is.Not.Null);
            Assert.That(termVote.Term, Is.EqualTo(term));
            Assert.That(termVote.VotedFor, Is.EqualTo(votedFor));
            Assert.That(p.LoadCommitIdx(), Is.EqualTo(commitIdx));
            Assert.That(p.LoadSnapshotDescriptor().Idx, Is.EqualTo(1));

            var logs = p.LoadLog();
            Assert.That(logs.Count, Is.EqualTo(entries.Count));
            for (var i = 0; i < entries.Count; ++i) {
                var log = logs[i];
                var expected = entries[i];
                Assert.That(log, Is.EqualTo(expected));
            }
        }
    }
}