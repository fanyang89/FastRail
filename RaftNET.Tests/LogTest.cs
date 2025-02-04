﻿namespace RaftNET.Tests;

public class LogTest {
    [Test]
    public void TestAppendRaftLogs() {
        var cfg = Messages.ConfigFromIds(1);
        var snp = new SnapshotDescriptor { Config = cfg };
        var log = new RaftLog(snp);

        Assert.Multiple(() => {
            Assert.That(log.LastIdx(), Is.EqualTo(0));
            Assert.That(log.LastConfIdx, Is.EqualTo(0));
        });

        // initial log with 3 entries
        log.Add(Messages.CreateFake());
        Assert.Multiple(() => {
            Assert.That(log.LastIdx(), Is.EqualTo(1));
            Assert.That(log[1].Fake, Is.Not.Null);
        });

        log.Add(Messages.CreateConfiguration(cfg));
        Assert.Multiple(() => {
            Assert.That(log.LastIdx(), Is.EqualTo(2));
            Assert.That(log.LastConfIdx, Is.EqualTo(2));
            Assert.That(log[2].Configuration, Is.Not.Null);
        });

        log.Add(Messages.CreateCommand("hello world"));
        Assert.Multiple(() => {
            Assert.That(log.LastIdx(), Is.EqualTo(3));
            Assert.That(log[3].Command, Is.Not.Null);
        });

        // re-append last entry with same term, should be no-op
        Assert.Multiple(() => {
            Assert.That(
                log.MaybeAppend([Messages.CreateFake(2, log.LastTerm())]),
                Is.EqualTo(2));
            Assert.That(log.LastIdx(), Is.EqualTo(3));
            Assert.That(log[3].Command, Is.Not.Null);
        });

        // re-append last entry with diff term, should replace it
        Assert.Multiple(() => {
            Assert.That(
                log.MaybeAppend([
                    Messages.CreateFake(2, log.LastTerm() + 1)
                ]),
                Is.EqualTo(2));
            Assert.That(log.LastIdx(), Is.EqualTo(2));
            Assert.That(log[2].Fake, Is.Not.Null);
            Assert.That(log.LastConfIdx, Is.EqualTo(0));
        });
    }
}
