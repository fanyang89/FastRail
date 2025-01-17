namespace RaftNET.Tests;

public class LeaderTransferLostTimeoutNowTest : FSMTestBase {
    [Test]
    public void TestLeaderTransferLostTimeoutNow() {
        var log = new Log(new SnapshotDescriptor {
            Idx = 0, Config = Messages.ConfigFromIds(A_ID, B_ID, C_ID)
        });
        var a = CreateFollower(A_ID, log.Clone());
        var b = CreateFollower(B_ID, log.Clone());
        var c = CreateFollower(C_ID, log.Clone());

        ElectionTimeout(a);
        Communicate(a, b, c);
        Assert.That(a.IsLeader, Is.True);

        var newCfg = Messages.ConfigFromIds(B_ID, C_ID);
        a.AddEntry(newCfg);

        CommunicateUntil(() => !a.IsLeader, a, b, c);

        var output = a.GetOutput();
        Assert.Multiple(() => {
            Assert.That(output.Messages, Has.Count.EqualTo(1));
            Assert.That(output.Messages.Last().Message.IsTimeoutNowRequest, Is.True);
            // ... and lose it.
            Assert.That(b.IsFollower, Is.True);
            Assert.That(c.IsFollower, Is.True);
        });

        ElectionTimeout(b);
        ElectionThreshold(c);
        Communicate(b, c);
        Assert.That(b.IsLeader, Is.True);
    }
}
