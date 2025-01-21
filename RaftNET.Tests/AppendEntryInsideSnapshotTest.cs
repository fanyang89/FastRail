namespace RaftNET.Tests;

public class AppendEntryInsideSnapshotTest : FSMTestBase {
    [Test]
    public void TestAppendEntryInsideSnapshot() {
        var log = new RaftLog(new SnapshotDescriptor {
            Idx = 0, Config = Messages.ConfigFromIds(A_ID, B_ID, C_ID)
        });

        var a = CreateFollower(A_ID, log.Clone());
        var b = CreateFollower(B_ID, log.Clone());
        var c = CreateFollower(C_ID, log.Clone());

        ElectionTimeout(a);
        Communicate(a, b, c);
        a.AddEntry(new Void());
        a.AddEntry(new Void());
        a.AddEntry(new Void());
        Communicate(a, b, c);

        a.AddEntry(new Void());
        var output = a.GetOutput();
        Assert.That(output.Messages, Has.Count.EqualTo(2));
        var append = output.Messages.Last().Message.AppendRequest;
        b.Step(A_ID, append);

        output = b.GetOutput();
        Assert.That(output.Messages, Has.Count.EqualTo(1));
        var reply = output.Messages.Last().Message.AppendResponse;
        a.Step(B_ID, reply);

        a.Tick();
        Communicate(a, b);

        a.Tick();
        output = a.GetOutput();
        Assert.That(output.Messages, Has.Count.EqualTo(1));
        append = output.Messages.Last().Message.AppendRequest;
        c.Step(A_ID, append);

        output = c.GetOutput();
        Assert.That(output.Messages, Has.Count.EqualTo(1));
        reply = output.Messages.Last().Message.AppendResponse;
        a.Step(C_ID, reply);

        output = a.GetOutput();
        Assert.That(output.Messages, Has.Count.EqualTo(1));
        append = output.Messages.Last().Message.AppendRequest;

        c.Step(A_ID, append);
        c.GetOutput();
        c.ApplySnapshot(Messages.LogSnapshot(c.RaftLog, c.LogLastIdx), 0, 0, true);

        a.AddEntry(new Void());
        a.Tick();
        Communicate(a, b, c);
        Assert.That(c.RaftLog.Empty, Is.False);
    }
}
