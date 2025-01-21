using RaftNET.FailureDetectors;

namespace RaftNET.Tests;

public class LeaderElectionOverwriteNewerLogTest : FSMTestBase {
    [Test]
    public void TestLeaderElectionOverwriteNewerLog() {
        var fd = new DiscreteFailureDetector();

        var cfg = Messages.ConfigFromIds(Id1, Id2, Id3, Id4, Id5);
        //                        idx  term
        // node 1:    log entries  1:   1                 won first got logs
        // node 2:    log entries  1:   1                 got logs from node 1
        // node 3:    log entries  1:   2                 wond second election
        // node 4:    log entries  (at term 2 voted for 3)
        // node 5:    log entries  (at term 2 voted for 3)
        var lep1 = new LogEntry { Term = 1, Idx = 1, Fake = new Void() };
        var log1 = new RaftLog(new SnapshotDescriptor { Config = cfg }, [lep1]);
        var fsm1 = new FSMDebug(Id1, 1, 0, log1, fd, FSMConfig);
        var lep2 = new LogEntry { Term = 1, Idx = 1, Fake = new Void() };
        var log2 = new RaftLog(new SnapshotDescriptor { Config = cfg }, [lep2]);
        var fsm2 = new FSMDebug(Id2, 1, 0, log2, fd, FSMConfig);

        var lep3 = new LogEntry { Term = 2, Idx = 1, Fake = new Void() };
        var log3 = new RaftLog(new SnapshotDescriptor { Config = cfg }, [lep3]);
        var fsm3 = new FSMDebug(Id3, 2, Id3, log3, fd, FSMConfig);

        var log4 = new RaftLog(new SnapshotDescriptor { Config = cfg });
        var fsm4 = new FSMDebug(Id4, 2, Id3, log4, fd, FSMConfig);
        var log5 = new RaftLog(new SnapshotDescriptor { Config = cfg });
        var fsm5 = new FSMDebug(Id5, 2, Id3, log5, fd, FSMConfig);

        fsm2.BecomeFollower(Id1);
        fsm4.BecomeFollower(Id3);
        fsm5.BecomeFollower(Id3);

        fd.MarkDead(Id3);
        MakeCandidate(fsm1);
        Assert.Multiple(() => {
            Assert.That(fsm1.IsCandidate, Is.True);
            Assert.That(fsm1.CurrentTerm, Is.EqualTo(2));
        });
        ElectionThreshold(fsm2);
        ElectionThreshold(fsm3);
        ElectionThreshold(fsm4);
        ElectionThreshold(fsm5);
        Communicate(fsm1, fsm2, fsm3, fsm4, fsm5);
        Assert.That(fsm1.IsFollower, Is.True);

        // Node 1 campaigns again with a higher term. This time it succeeds.
        MakeCandidate(fsm1);
        Communicate(fsm1, fsm2, fsm3, fsm4, fsm5);

        Assert.Multiple(() => {
            Assert.That(CompareLogEntries(fsm1.RaftLog, fsm3.RaftLog, 1, 2), Is.True);
            Assert.That(CompareLogEntries(fsm1.RaftLog, fsm4.RaftLog, 1, 2), Is.True);
            Assert.That(CompareLogEntries(fsm1.RaftLog, fsm5.RaftLog, 1, 2), Is.True);
        });
    }
}
