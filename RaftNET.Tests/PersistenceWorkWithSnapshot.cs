namespace RaftNET.Tests;

public class PersistenceWorkWithSnapshot {
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
    public void StoreLoad() {
        _persistence.StoreSnapshotDescriptor(new SnapshotDescriptor { Idx = 5 }, _count);
        var snapshot = _persistence.LoadSnapshotDescriptor();
        Assert.That(snapshot.Idx, Is.EqualTo(5));
    }

    [Test]
    public void Preserve1() {
        _persistence.StoreSnapshotDescriptor(new SnapshotDescriptor { Idx = 5 }, 10);
        var logs = _persistence.LoadLog();
        Assert.That(logs.Count, Is.EqualTo(_count));
    }

    [Test]
    public void Preserve2() {
        var snapshot = new SnapshotDescriptor { Idx = 5 };
        _persistence.StoreSnapshotDescriptor(snapshot, 5);
        var logs = _persistence.LoadLog();
        Assert.That(logs.Count, Is.EqualTo(5));
    }

    [Test]
    public void Preserve3() {
        var snapshot = new SnapshotDescriptor { Idx = 5 };
        _persistence.StoreSnapshotDescriptor(snapshot, 0);
        var logs = _persistence.LoadLog();
        Assert.That(logs.Count, Is.EqualTo(5));
    }
}