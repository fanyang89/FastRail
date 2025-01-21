using RaftNET.FailureDetectors;

namespace RaftNET.Tests;

public class OldMessageTest : FSMTestBase {
    [Test]
    public void TestOldMessages() {
        var fd = new DiscreteFailureDetector();
        var cfg = Messages.ConfigFromIds(Id1, Id2, Id3);
        var log1 = new RaftLog(new SnapshotDescriptor { Config = cfg });
        var fsm1 = new FSMDebug(Id1, 0, 0, log1, fd, FSMConfig);
        var log2 = new RaftLog(new SnapshotDescriptor { Config = cfg });
        var fsm2 = new FSMDebug(Id2, 0, 0, log2, fd, FSMConfig);
        var log3 = new RaftLog(new SnapshotDescriptor { Config = cfg });
        var fsm3 = new FSMDebug(Id3, 0, 0, log3, fd, FSMConfig);

        MakeCandidate(fsm1);
        Communicate(fsm1, fsm2, fsm3); // Term 1
        Assert.That(fsm1.IsLeader, Is.True);
        fd.MarkDead(Id1);
        ElectionThreshold(fsm3);
        MakeCandidate(fsm2);
        Assert.That(fsm2.IsCandidate, Is.True);
        Communicate(fsm2, fsm1, fsm3); // Term 2
        fd.MarkAlive(Id1);
        Assert.That(fsm2.IsLeader, Is.True);
        fd.MarkDead(Id2);
        ElectionThreshold(fsm3);
        MakeCandidate(fsm1);
        Communicate(fsm1, fsm2, fsm3); // Term 3
        Assert.That(fsm1.IsLeader, Is.True);
        fd.MarkAlive(Id2);
        Assert.That(fsm1.CurrentTerm, Is.EqualTo(3));

        fsm1.Step(Id2, new AppendRequest { CurrentTerm = 2, PrevLogIdx = 2, PrevLogTerm = 2 });

        fsm1.AddEntry("4");
        Communicate(fsm2, fsm1, fsm3);

        var res1 = fsm1.RaftLog;
        for (ulong i = 1; i < 5; ++i) {
            Assert.That(res1[i].Idx, Is.EqualTo(i));
            if (i < 4) {
                Assert.Multiple(() => {
                    Assert.That(res1[i].Term, Is.EqualTo(i));
                    Assert.That(res1[i].DataCase, Is.EqualTo(LogEntry.DataOneofCase.Fake));
                });
            } else {
                Assert.Multiple(() => {
                    Assert.That(res1[i].Term, Is.EqualTo(3));
                    Assert.That(res1[i].DataCase, Is.EqualTo(LogEntry.DataOneofCase.Command));
                });
            }
        }

        Assert.Multiple(() => {
            Assert.That(CompareLogEntries(res1, fsm2.RaftLog, 1, 4));
            Assert.That(CompareLogEntries(res1, fsm3.RaftLog, 1, 4));
        });
    }
}
