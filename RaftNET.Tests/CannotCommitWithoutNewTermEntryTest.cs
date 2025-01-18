using RaftNET.FailureDetectors;

namespace RaftNET.Tests;

public class CannotCommitWithoutNewTermEntryTest : FSMTestBase {
    [Test]
    public void TestCannotCommitWithoutNewTermEntry() {
        var fd = new DiscreteFailureDetector();
        var cfg = Messages.ConfigFromIds(Id1, Id2, Id3, Id4, Id5);

        var log1 = new Log(new SnapshotDescriptor { Config = cfg });
        var fsm1 = new FSMDebug(Id1, 0, 0, log1, fd, FSMConfig);
        var log2 = new Log(new SnapshotDescriptor { Config = cfg });
        var fsm2 = new FSMDebug(Id2, 0, 0, log2, fd, FSMConfig);
        var log3 = new Log(new SnapshotDescriptor { Config = cfg });
        var fsm3 = new FSMDebug(Id3, 0, 0, log3, fd, FSMConfig);
        var log4 = new Log(new SnapshotDescriptor { Config = cfg });
        var fsm4 = new FSMDebug(Id4, 0, 0, log4, fd, FSMConfig);
        var log5 = new Log(new SnapshotDescriptor { Config = cfg });
        var fsm5 = new FSMDebug(Id5, 0, 0, log5, fd, FSMConfig);

        MakeCandidate(fsm1);
        Communicate(fsm1, fsm2, fsm3, fsm4, fsm5);
        Assert.That(fsm1.IsLeader, Is.True);
        Communicate(fsm1, fsm2, fsm3, fsm4, fsm5);

        for (var i = 2; i < 4; ++i) {
            fsm1.AddEntry(i.ToString());
        }
        Communicate(fsm1, fsm2);

        fd.MarkDead(Id1);
        MakeCandidate(fsm2);
        ElectionThreshold(fsm3);
        ElectionThreshold(fsm4);
        ElectionThreshold(fsm5);
        Assert.That(fsm2.IsCandidate, Is.True);
        Communicate(fsm2, fsm1, fsm3, fsm4, fsm5);
        fd.MarkAlive(Id1);
        Assert.That(fsm2.IsLeader, Is.True);

        fsm2.AddEntry("5");
        Communicate(fsm2, fsm1, fsm3, fsm4, fsm5);
        Assert.Multiple(() => {
            Assert.That(CompareLogEntries(fsm2.Log, fsm1.Log, 1, 5));
            Assert.That(CompareLogEntries(fsm2.Log, fsm3.Log, 1, 5));
            Assert.That(CompareLogEntries(fsm2.Log, fsm4.Log, 1, 5));
            Assert.That(CompareLogEntries(fsm2.Log, fsm5.Log, 1, 5));
        });
    }
}
