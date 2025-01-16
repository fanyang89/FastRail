namespace RaftNET.Tests;

public class LogLastConfIdxTest : FSMTestBase {
    private const ulong ID1 = 1;

    [Test]
    public void TestLogLastConfIdx() {
        // last_conf_idx, prev_conf_idx are initialized correctly,
        // and maintained during truncate head/truncate tail
        var cfg = Messages.ConfigFromIds(ID1);
        var log = new Log(new SnapshotDescriptor { Config = cfg });
        Assert.That(log.LastConfIdx, Is.EqualTo(0));
        log.Add(new LogEntry { Configuration = cfg, Term = log.LastTerm(), Idx = log.LastIdx() });
        Assert.That(log.LastConfIdx, Is.EqualTo(1));
        log.Add(new LogEntry { Dummy = new Void(), Term = log.LastTerm(), Idx = log.LastIdx() });
        log.Add(new LogEntry { Configuration = cfg, Term = log.LastTerm(), Idx = log.LastIdx() });
        Assert.That(log.LastConfIdx, Is.EqualTo(3));
        // apply snapshot truncates the log and resets last_conf_idx()
        log.ApplySnapshot(Messages.LogSnapshot(log, log.LastIdx()), 0, 0);
        Assert.That(log.LastConfIdx, Is.EqualTo(log.GetSnapshot().Idx));
        // log::last_term() is maintained correctly by truncate_head/truncate_tail() (snapshotting)
        Assert.That(log.LastTerm(), Is.EqualTo(log.GetSnapshot().Term));
        Assert.That(log.TermFor(log.GetSnapshot().Idx), Is.Not.Null);
        Assert.That(log.TermFor(log.GetSnapshot().Idx), Is.EqualTo(log.GetSnapshot().Term));
        Assert.That(log.TermFor(log.LastIdx() - 1), Is.Null);
        log.Add(new LogEntry { Dummy = new Void(), Term = log.LastTerm(), Idx = log.LastIdx() });
        Assert.That(log.TermFor(log.LastIdx()), Is.Not.Null);
        log.Add(new LogEntry { Dummy = new Void(), Term = log.LastTerm(), Idx = log.LastIdx() });
        const int gap = 10;
        // apply_snapshot with a log gap, this should clear all log
        // entries, despite that trailing is given, a gap
        // between old log entries and a snapshot would violate
        // log continuity.
        log.ApplySnapshot(Messages.LogSnapshot(log, log.LastIdx() + gap), gap * 2, int.MaxValue);
        Assert.That(log.Empty, Is.True);
        Assert.That(log.NextIdx(), Is.EqualTo(log.GetSnapshot().Idx + 1));
        log.Add(new LogEntry { Dummy = new Void(), Term = log.LastTerm(), Idx = log.LastIdx() });
        Assert.That(log.InMemorySize(), Is.EqualTo(1));
        log.Add(new LogEntry { Dummy = new Void(), Term = log.LastTerm(), Idx = log.LastIdx() });
        Assert.That(log.InMemorySize(), Is.EqualTo(2));
        // Set trailing longer than the length of the log.
        log.ApplySnapshot(Messages.LogSnapshot(log, log.LastIdx()), 3, int.MaxValue);
        Assert.That(log.InMemorySize(), Is.EqualTo(2));
        // Set trailing the same length as the current log length
        log.Add(new LogEntry { Dummy = new Void(), Term = log.LastTerm(), Idx = log.LastIdx() });
        Assert.That(log.InMemorySize(), Is.EqualTo(3));
        log.ApplySnapshot(Messages.LogSnapshot(log, log.LastIdx()), 3, int.MaxValue);
        Assert.That(log.InMemorySize(), Is.EqualTo(3));
        Assert.That(log.LastConfIdx, Is.EqualTo(log.GetSnapshot().Idx));
        log.Add(new LogEntry { Dummy = new Void(), Term = log.LastTerm(), Idx = log.LastIdx() });
        // Set trailing shorter than the length of the log
        log.ApplySnapshot(Messages.LogSnapshot(log, log.LastIdx()), 1, int.MaxValue);
        Assert.That(log.InMemorySize(), Is.EqualTo(1));
        // check that configuration from snapshot is used and not config entries from a trailing
        log.Add(new LogEntry { Configuration = cfg, Term = log.LastTerm(), Idx = log.LastIdx() });
        log.Add(new LogEntry { Configuration = cfg, Term = log.LastTerm(), Idx = log.LastIdx() });
        log.Add(new LogEntry { Dummy = new Void(), Term = log.LastTerm(), Idx = log.LastIdx() });
        var snpIdx = log.LastIdx();
        log.ApplySnapshot(Messages.LogSnapshot(log, snpIdx), 10, int.MaxValue);
        Assert.That(log.LastConfIdx, Is.EqualTo(snpIdx));
        // Check that configuration from the log is used if it has higher index then snapshot idx
        log.Add(new LogEntry { Dummy = new Void(), Term = log.LastTerm(), Idx = log.LastIdx() });
        snpIdx = log.LastIdx();
        log.Add(new LogEntry { Configuration = cfg, Term = log.LastTerm(), Idx = log.LastIdx() });
        log.Add(new LogEntry { Configuration = cfg, Term = log.LastTerm(), Idx = log.LastIdx() });
        log.ApplySnapshot(Messages.LogSnapshot(log, snpIdx), 10, int.MaxValue);
        Assert.That(log.LastConfIdx, Is.EqualTo(log.LastIdx()));
    }
}
