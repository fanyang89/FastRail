using RaftNET.FailureDetectors;
using RaftNET.Records;

namespace RaftNET.Tests;

// Test we can transition from a single-server configuration
// {A} to a single server configuration {B}
public class ConfigurationChangeAToBTest : FSMTestBase {
    [Test]
    public void TestConfigurationChangeAToB() {
        var log = new Log(new SnapshotDescriptor {
            Idx = 0,
            Config = Messages.ConfigFromIds(A_ID)
        });
        var a = CreateFollower(A_ID, log);
        ElectionTimeout(a);
        Assert.That(a.IsLeader, Is.True);
        // Let's have a non-empty log at A
        a.AddEntry(new Dummy());

        var b = CreateFollower(B_ID, log.Clone());
        a.AddEntry(Messages.ConfigFromIds(B_ID));

        Communicate(a, b);
        Assert.Multiple(() => {
            Assert.That(a.CurrentTerm, Is.EqualTo(1));
            Assert.That(a.IsFollower, Is.True);
            Assert.That(b.CurrentTerm, Is.EqualTo(2));
            Assert.That(b.IsLeader, Is.True);
            Assert.That(b.GetConfiguration().IsJoint, Is.False);
            Assert.That(b.GetConfiguration().Current, Has.Count.EqualTo(1));
            Assert.That(b.GetConfiguration().Current.Any(x => x.ServerAddress.ServerId == B_ID), Is.True);
        });

        log = new Log(new SnapshotDescriptor {
            Idx = b.LogLastIdx,
            Term = b.LogLastTerm,
            Config = b.GetConfiguration()
        });
        log.Add(b.AddEntry(Messages.ConfigFromIds(A_ID)));
        log.StableTo(log.LastIdx());
        var b1 = new FSMDebug(B_ID, b.CurrentTerm, B_ID, log, new TrivialFailureDetector(), FSMConfig);
        ElectionTimeout(b1);
        Communicate(a, b1);
        Assert.That(b1.IsFollower, Is.True);
        ElectionTimeout(a);
        Assert.That(a.IsLeader, Is.True);
        ElectionTimeout(b1);
        Assert.Multiple(() => {
            Assert.That(b1.IsFollower, Is.True);
            Assert.That(b1.GetOutput().Messages, Is.Empty);
        });
    }
}