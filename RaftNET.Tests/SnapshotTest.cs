namespace RaftNET.Tests;

public class SnapshotTest : FSMTestBase {
    [Test]
    public void RejectOutdatedRemoteSnapshotTest() {
        var cfg = Messages.ConfigFromIds(A_ID, B_ID);
        var log = new Log(new SnapshotDescriptor {
            Idx = 0, Config = cfg
        });
        var a = CreateFollower(A_ID, log.Clone());
        var b = CreateFollower(B_ID, log.Clone());
        ElectionTimeout(a);
        Communicate(a, b);
        Assert.That(a.IsLeader, Is.True);

        a.AddEntry(new Void());
        a.AddEntry(new Void());
        Communicate(a, b);

        ulong snpIdx = 1;
        Assert.That(b.LogLastIdx, Is.GreaterThan(snpIdx));
        var snpTerm = b.Log.TermFor(snpIdx);
        Assert.That(snpTerm, Is.Not.Null);
        var snp = new SnapshotDescriptor {
            Idx = snpIdx, Term = snpTerm.Value, Config = cfg
        };
        Assert.Multiple(() => {
            Assert.That(b.ApplySnapshot(snp, 0, 0, false), Is.False);
            Assert.That(b.ApplySnapshot(snp, 0, 0, true), Is.True);
        });
    }
}
