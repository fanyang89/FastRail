using RaftNET.Records;

namespace RaftNET.Tests;

public class SingleNodeQuietTest : FSMTestBase {
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
}