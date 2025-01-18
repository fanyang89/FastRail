using RaftNET.FailureDetectors;

namespace RaftNET.Tests;

public class SingleNodeTest : FSMTestBase {
    [Test]
    public void TestSingleNodeCommit() {
        var cfg = Messages.ConfigFromIds(Id1);
        var log = new Log(new SnapshotDescriptor { Config = cfg });
        var fsm = new FSMDebug(Id1, 0, 0, log, new TrivialFailureDetector(), FSMConfig);

        Assert.That(fsm.IsLeader, Is.True);
        var output = fsm.GetOutput();
        Assert.That(output.LogEntries, Has.Count.EqualTo(1));
        var lep = output.LogEntries.Last();
        Assert.That(lep.DataCase, Is.EqualTo(LogEntry.DataOneofCase.Fake));
        output = fsm.GetOutput();
        Assert.That(output.Committed, Has.Count.EqualTo(1));

        // Add 1st data entry
        fsm.AddEntry("1");
        output = fsm.GetOutput();
        Assert.That(output.LogEntries, Has.Count.EqualTo(1));
        output = fsm.GetOutput();
        Assert.That(output.Committed, Has.Count.EqualTo(1));

        // Add 2nd data entry
        fsm.AddEntry("2");
        output = fsm.GetOutput();
        Assert.That(output.LogEntries, Has.Count.EqualTo(1));
        output = fsm.GetOutput();
        Assert.That(output.Committed, Has.Count.EqualTo(1));
    }

    [Test]
    public void TestSingleNodePreCandidate() {
        var cfg = Messages.ConfigFromIds(Id1);
        var log = new Log(new SnapshotDescriptor { Config = cfg });
        var fsm1 = new FSMDebug(Id1, 0, 0, log, new TrivialFailureDetector(), FSMPreVoteConfig);
        Assert.That(fsm1.IsLeader, Is.True);
    }
}
