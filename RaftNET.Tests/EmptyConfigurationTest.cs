namespace RaftNET.Tests;

// When a server is joining an existing cluster, its configuration is empty.
// The leader sends its configuration over in AppendEntries or ApplySnapshot RPC.
public class EmptyConfigurationTest : FSMTestBase {
    [Test]
    public void TestEmptyConfiguration() {
        var cfg = new Configuration();
        var log = new RaftLog(new SnapshotDescriptor { Config = cfg, Idx = 0 });

        var follower = CreateFollower(Id1, log);
        Assert.That(follower.IsFollower);
        ElectionTimeout(follower);
        Assert.That(follower.IsFollower);

        var output = follower.GetOutput();
        Assert.Multiple(() => {
            Assert.That(output.LogEntries, Is.Empty);
            Assert.That(output.Messages, Is.Empty);
            Assert.That(follower.CurrentTerm, Is.EqualTo(0));
        });

        var log2 = new RaftLog(new SnapshotDescriptor { Idx = 0, Config = Messages.ConfigFromIds(Id2) });
        var leader = CreateFollower(Id2, log2);
        ElectionTimeout(leader);
        Assert.That(leader.IsLeader);
        Assert.Throws<ArgumentException>(() => {
            leader.AddEntry(new Configuration());
        });
        leader.AddEntry(Messages.ConfigFromIds(Id1, Id2));

        Communicate(leader, follower);
        Assert.Multiple(() => {
            Assert.That(follower.CurrentTerm, Is.EqualTo(1));
            Assert.That(follower.InMemoryLogSize, Is.EqualTo(leader.InMemoryLogSize));
            Assert.That(leader.GetConfiguration().IsJoint(), Is.False);
        });
    }
}
