using RaftNET.FailureDetectors;
using RaftNET.Records;
using RaftNET.Replication;

namespace RaftNET.Tests;

public class QuiteTest : FSMTestBase {
    [Test]
    public void TestSingleNodeIsQuiet() {
        var cfg = Messages.ConfigFromIds(Id1);
        var log = new Log(new SnapshotDescriptor { Config = cfg });
        var fsm = CreateFollower(Id1, log);
        ElectionTimeout(fsm);
        Assert.That(fsm.IsLeader, Is.True);

        fsm.GetOutput();
        fsm.AddEntry(new Dummy());
        Assert.That(fsm.GetOutput().Messages, Is.Empty);

        fsm.Tick();
        Assert.That(fsm.GetOutput().Messages, Is.Empty);
    }

    [Test]
    public void TestSnapshotFollowerIsQuite() {
        var cfg = Messages.ConfigFromIds(Id1, Id2);
        var log = new Log(new SnapshotDescriptor { Idx = 999, Config = cfg });

        log.Add(new LogEntry { Term = 10, Idx = 1000 });
        log.StableTo(log.LastIdx());

        var fsm = new FSMDebug(Id1, 10, 0, log, new TrivialFailureDetector(), FSMConfig);

        ElectionTimeout(fsm);
        fsm.Step(Id2, new VoteResponse { CurrentTerm = fsm.CurrentTerm, VoteGranted = true });
        Assert.That(fsm.IsLeader, Is.True);

        fsm.GetOutput();

        fsm.Step(Id2,
            new AppendResponse {
                CurrentTerm = fsm.CurrentTerm, CommitIdx = 1,
                Rejected = new AppendRejected { NonMatchingIdx = 1000, LastIdx = 1 }
            });

        var progress = fsm.GetProgress(Id2);
        Assert.That(progress, Is.Not.Null);
        Assert.That(progress.State, Is.EqualTo(FollowerProgressState.Snapshot));

        fsm.GetOutput();

        for (var i = 0; i < 100; ++i) {
            fsm.Tick();
            Assert.That(fsm.GetOutput().Messages, Is.Empty);
        }
    }
}
