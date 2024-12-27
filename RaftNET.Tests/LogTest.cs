using Microsoft.Extensions.Logging.Abstractions;

namespace RaftNET.Tests;

public class LogTest {
    private Log _log;
    private Configuration _cfg;

    [SetUp]
    public void SetUp() {
        _cfg = Messages.ConfigFromIds(1);
        var snp = new SnapshotDescriptor {
            Config = _cfg
        };
        _log = new Log(snp, [], new NullLogger<Log>());
    }

    [Test]
    public void TestAppendRaftLogs() {
        Assert.Multiple(() => {
            Assert.That(_log.LastIdx(), Is.EqualTo(0));
            Assert.That(_log.LastConfIdx(), Is.EqualTo(0));
        });

        // initial log with 3 entries
        _log.Add(Messages.CreateDummy());
        Assert.Multiple(() => {
            Assert.That(_log.LastIdx(), Is.EqualTo(1));
            Assert.That(_log[1].Dummy, Is.Not.Null);
        });

        _log.Add(Messages.CreateConfiguration(_cfg));
        Assert.Multiple(() => {
            Assert.That(_log.LastIdx(), Is.EqualTo(2));
            Assert.That(_log.LastConfIdx(), Is.EqualTo(2));
            Assert.That(_log[2].Configuration, Is.Not.Null);
        });

        _log.Add(Messages.CreateCommand("hello world"));
        Assert.Multiple(() => {
            Assert.That(_log.LastIdx(), Is.EqualTo(3));
            Assert.That(_log[3].Command, Is.Not.Null);
        });

        // re-append last entry with same term, should be no-op
        Assert.Multiple(() => {
            Assert.That(
                _log.MaybeAppend([Messages.CreateDummy(2, _log.LastTerm())]),
                Is.EqualTo(2));
            Assert.That(_log.LastIdx(), Is.EqualTo(3));
            Assert.That(_log[3].Command, Is.Not.Null);
        });

        // re-append last entry with diff term, should replace it
        Assert.Multiple(() => {
            Assert.That(
                _log.MaybeAppend([
                    Messages.CreateDummy(2, _log.LastTerm() + 1)
                ]),
                Is.EqualTo(2));
            Assert.That(_log.LastIdx(), Is.EqualTo(2));
            Assert.That(_log[2].Dummy, Is.Not.Null);
            Assert.That(_log.LastConfIdx(), Is.EqualTo(0));
        });
    }
}