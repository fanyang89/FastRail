namespace RaftNET.Tests;

public class LogTest {
    private Log log_;
    private Configuration cfg_;

    [SetUp]
    public void SetUp() {
        cfg_ = Messages.ConfigFromIds(1);
        var snp = new SnapshotDescriptor {
            Config = cfg_
        };
        log_ = new Log(snp, new List<LogEntry>());
    }

    [Test]
    public void TestAppendRaftLogs() {
        Assert.Multiple(() => {
            Assert.That(log_.LastIdx(), Is.EqualTo(0));
            Assert.That(log_.LastConfIdx(), Is.EqualTo(0));
        });

        // initial log with 3 entries
        log_.Add(Messages.CreateDummy());
        Assert.Multiple(() => {
            Assert.That(log_.LastIdx(), Is.EqualTo(1));
            Assert.That(log_[1].Dummy, Is.Not.Null);
        });

        log_.Add(Messages.CreateConfiguration(cfg_));
        Assert.Multiple(() => {
            Assert.That(log_.LastIdx(), Is.EqualTo(2));
            Assert.That(log_.LastConfIdx(), Is.EqualTo(2));
            Assert.That(log_[2].Configuration, Is.Not.Null);
        });

        log_.Add(Messages.CreateCommand("hello world"));
        Assert.Multiple(() => {
            Assert.That(log_.LastIdx(), Is.EqualTo(3));
            Assert.That(log_[2].Command, Is.Not.Null);
        });
    }
}