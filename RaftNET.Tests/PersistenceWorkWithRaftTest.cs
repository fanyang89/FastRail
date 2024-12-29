using RaftNET.Persistence;

namespace RaftNET.Tests;

public class PersistenceWorkWithRaftTest {
    private RocksPersistence _persistence;
    private readonly ulong _startIdx = 1;
    private readonly ulong _count = 10;
    private readonly ulong _termOffset = 100;

    [SetUp]
    public void Setup() {
        var tempDir = Directory.CreateTempSubdirectory();
        _persistence = new RocksPersistence(tempDir.FullName);

        var entries = new List<LogEntry>();
        for (var i = _startIdx; i < _startIdx + _count; ++i) {
            entries.Add(new LogEntry {
                Idx = i,
                Term = i + _termOffset
            });
        }
        _persistence.StoreLogEntries(entries);
    }

    [TearDown]
    public void TearDown() {
        _persistence.Dispose();
    }


    [Test]
    public void LoadLogs() {
        var logs = _persistence.LoadLog();
        for (ulong i = 0; i < (ulong)logs.Count; ++i) {
            var log = logs[(int)i];
            Assert.That(log.Idx, Is.EqualTo(i + _startIdx));
            Assert.That(log.Term, Is.EqualTo(i + _startIdx + _termOffset));
        }
    }

    [Test]
    public void TruncateLogs1() {
        _persistence.TruncateLog(20);
        var logs = _persistence.LoadLog();
        Assert.That(logs.Count, Is.EqualTo(_count));
    }

    [Test]
    public void TruncateLogs2() {
        _persistence.TruncateLog(5);
        var logs = _persistence.LoadLog();
        Assert.That(logs.Count, Is.EqualTo(4));
        for (ulong i = 0; i < (ulong)logs.Count; i++) {
            var log = logs[(int)i];
            Assert.That(log.Idx, Is.EqualTo(i + _startIdx));
            Assert.That(log.Term, Is.EqualTo(i + _startIdx + _termOffset));
        }
    }

    [Test]
    public void TruncateLogs3() {
        _persistence.TruncateLog(0);
        var logs = _persistence.LoadLog();
        Assert.That(logs.Count, Is.Zero);
    }
}