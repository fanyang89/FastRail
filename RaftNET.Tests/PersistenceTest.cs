using RaftNET.Persistence;

namespace RaftNET.Tests;

public class PersistenceTest {
    private const ulong StartIdx = 1;
    private const ulong Count = 10;
    private const ulong TermOffset = 100;
    private RocksPersistence _persistence;

    [SetUp]
    public void Setup() {
        var tempDir = Directory.CreateTempSubdirectory();
        _persistence = new RocksPersistence(tempDir.FullName);
        var entries = new List<LogEntry>();
        for (var i = StartIdx; i < StartIdx + Count; ++i) {
            entries.Add(new LogEntry { Idx = i, Term = i + TermOffset });
        }
        _persistence.StoreLogEntries(entries);
    }

    [TearDown]
    public void TearDown() {
        _persistence.Dispose();
    }

    [Test]
    public void TestRaftLoadLogs() {
        var logs = _persistence.LoadLog();

        for (ulong i = 0; i < (ulong)logs.Count; ++i) {
            var log = logs[(int)i];
            Assert.Multiple(() => {
                Assert.That(log.Idx, Is.EqualTo(i + StartIdx));
                Assert.That(log.Term, Is.EqualTo(i + StartIdx + TermOffset));
            });
        }
    }

    [Test]
    public void TestRaftTruncateLogs1() {
        _persistence.TruncateLog(20);
        var logs = _persistence.LoadLog();
        Assert.That(logs, Has.Count.EqualTo(Count));
    }

    [Test]
    public void TestRaftTruncateLogs2() {
        _persistence.TruncateLog(5);
        var logs = _persistence.LoadLog();
        Assert.That(logs, Has.Count.EqualTo(4));

        for (ulong i = 0; i < (ulong)logs.Count; i++) {
            var log = logs[(int)i];
            Assert.Multiple(() => {
                Assert.That(log.Idx, Is.EqualTo(i + StartIdx));
                Assert.That(log.Term, Is.EqualTo(i + StartIdx + TermOffset));
            });
        }
    }

    [Test]
    public void TestRaftTruncateLogs3() {
        _persistence.TruncateLog(0);
        var logs = _persistence.LoadLog();
        Assert.That(logs, Is.Empty);
    }

    [Test]
    public void TestSnapshotStoreLoad() {
        _persistence.StoreSnapshotDescriptor(new SnapshotDescriptor { Idx = 5 }, Count);
        var snapshot = _persistence.LoadSnapshotDescriptor();
        Assert.That(snapshot, Is.Not.Null);
        Assert.That(snapshot.Idx, Is.EqualTo(5));
    }

    [Test]
    public void TestSnapshotPreserve1() {
        _persistence.StoreSnapshotDescriptor(new SnapshotDescriptor { Idx = 5 }, 10);
        var logs = _persistence.LoadLog();
        Assert.That(logs, Has.Count.EqualTo(Count));
    }

    [Test]
    public void TestSnapshotPreserve2() {
        var snapshot = new SnapshotDescriptor { Idx = 5 };
        _persistence.StoreSnapshotDescriptor(snapshot, 5);
        var logs = _persistence.LoadLog();
        Assert.That(logs, Has.Count.EqualTo(5));
    }

    [Test]
    public void TestSnapshotPreserve3() {
        var snapshot = new SnapshotDescriptor { Idx = 5 };
        _persistence.StoreSnapshotDescriptor(snapshot, 0);
        var logs = _persistence.LoadLog();
        Assert.That(logs, Has.Count.EqualTo(5));
    }

    [Test]
    public void TestPersistenceWorks() {
        var dataDir = Directory.CreateTempSubdirectory();
        const ulong term = 1;
        const ulong votedFor = 1;
        const ulong commitIdx = 1;
        var entries = new List<LogEntry>();
        {
            using var p = new RocksPersistence(dataDir.FullName);
            Assert.That(p.LoadTermVote(), Is.Null);

            p.StoreTermVote(term, votedFor);
            p.StoreCommitIdx(commitIdx);

            var snapshot = new SnapshotDescriptor { Idx = 1 };
            p.StoreSnapshotDescriptor(snapshot, 0);

            for (ulong i = 2; i < 12; ++i)
                entries.Add(new LogEntry { Idx = i, Term = i + 100 });

            p.StoreLogEntries(entries);
        }
        {
            using var p = new RocksPersistence(dataDir.FullName);
            var termVote = p.LoadTermVote();
            Assert.That(termVote, Is.Not.Null);
            Assert.Multiple(() => {
                Assert.That(termVote.Term, Is.EqualTo(term));
                Assert.That(termVote.VotedFor, Is.EqualTo(votedFor));
                Assert.That(p.LoadCommitIdx(), Is.EqualTo(commitIdx));
            });
            var snapshot = p.LoadSnapshotDescriptor();
            Assert.That(snapshot, Is.Not.Null);
            Assert.That(snapshot.Idx, Is.EqualTo(1));

            var logs = p.LoadLog();
            Assert.That(logs, Has.Count.EqualTo(entries.Count));

            for (var i = 0; i < entries.Count; ++i) {
                var log = logs[i];
                var expected = entries[i];
                Assert.That(log, Is.EqualTo(expected));
            }
        }
    }
}
