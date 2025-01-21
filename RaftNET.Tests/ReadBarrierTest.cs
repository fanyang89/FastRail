using RaftNET.Exceptions;

namespace RaftNET.Tests;

public class ReadBarrierTest : FSMTestBase {
    [Test]
    public void TestReadBarrier() {
        var log = new RaftLog(new SnapshotDescriptor {
            Idx = 0, Config = Messages.ConfigFromIds(A_ID, B_ID, C_ID, D_ID)
        });
        var a = CreateFollower(A_ID, log.Clone());
        var b = CreateFollower(B_ID, log.Clone());
        var c = CreateFollower(C_ID, log.Clone());
        var d = CreateFollower(D_ID, log.Clone());
        var e = CreateFollower(E_ID, log.Clone());

        ElectionTimeout(a);
        Communicate(a, b, c, d);
        Assert.That(a.IsLeader, Is.True);

        a.Tick();
        Communicate(a, b, c, d);
        Assert.Throws<OutsideConfigurationException>(() => a.StartReadBarrier(E_ID));

        var rid = a.StartReadBarrier(A_ID);
        Assert.That(rid, Is.Not.Null);

        // Check that read_quorum was broadcast to other nodes
        var output = a.GetOutput();
        Assert.That(output.Messages, Has.Count.EqualTo(3));
        Assert.Multiple(() => {
            Assert.That(output.Messages[0].Message.IsReadQuorumRequest, Is.True);
            Assert.That(output.Messages[1].Message.IsReadQuorumRequest, Is.True);
            Assert.That(output.Messages[2].Message.IsReadQuorumRequest, Is.True);
        });

        // Check that it gets re-broadcast on leader's tick
        a.Tick();
        output = a.GetOutput();
        Assert.That(output.Messages, Has.Count.EqualTo(3));
        Assert.Multiple(() => {
            Assert.That(output.Messages[0].Message.IsReadQuorumRequest, Is.True);
            Assert.That(output.Messages[1].Message.IsReadQuorumRequest, Is.True);
            Assert.That(output.Messages[2].Message.IsReadQuorumRequest, Is.True);
        });

        var readQuorumMessage = output.Messages.First().Message.ReadQuorumRequest;
        // check that read id is correct
        Assert.That(readQuorumMessage.Id, Is.EqualTo(rid.Value.Item1));

        // Check that a leader ignores read_barrier with its own term
        a.Step(B_ID, readQuorumMessage);
        output = b.GetOutput();
        Assert.That(output.Messages, Has.Count.EqualTo(0));

        // Check that a follower replies to read_barrier with read_quorum_reply
        b.Step(A_ID, readQuorumMessage);
        output = b.GetOutput();
        Assert.That(output.Messages, Has.Count.EqualTo(1));
        Assert.That(output.Messages.First().Message.IsReadQuorumResponse, Is.True);

        var readQuorumResponseMessage = output.Messages.First().Message.ReadQuorumResponse;

        // Ack barrier from B and check that this is not enough to complete a read
        a.Step(B_ID, readQuorumResponseMessage);
        output = a.GetOutput();
        Assert.That(output.MaxReadIdWithQuorum, Is.Null);

        // Ack from B one more time and check that ack is not counted twice
        a.Step(B_ID, readQuorumResponseMessage);
        output = a.GetOutput();
        Assert.That(output.MaxReadIdWithQuorum, Is.Null);

        // Ack from C and check that the read barrier is completed
        a.Step(C_ID, readQuorumResponseMessage);
        output = a.GetOutput();
        Assert.That(output.MaxReadIdWithQuorum, Is.Not.Zero);

        // Enter joint config
        var newCfg = Messages.ConfigFromIds(A_ID, E_ID);
        a.AddEntry(newCfg);
        // Process log storing event and drop append_entries messages
        a.GetOutput();

        // start read barrier
        rid = a.StartReadBarrier(A_ID);
        Assert.That(rid, Is.Not.Null);

        // check that read_barrier is broadcast to all nodes
        output = a.GetOutput();
        Assert.That(output.Messages, Has.Count.EqualTo(4));
        Assert.Multiple(() => {
            Assert.That(output.Messages[0].Message.IsReadQuorumRequest, Is.True);
            Assert.That(output.Messages[1].Message.IsReadQuorumRequest, Is.True);
            Assert.That(output.Messages[2].Message.IsReadQuorumRequest, Is.True);
            Assert.That(output.Messages[3].Message.IsReadQuorumRequest, Is.True);
        });

        // Ack in only old quorum and check that the read is not completed
        a.Step(B_ID, new ReadQuorumResponse { CurrentTerm = a.CurrentTerm, CommitIdx = 0, Id = rid.Value.Item1 });
        a.Step(C_ID, new ReadQuorumResponse { CurrentTerm = a.CurrentTerm, CommitIdx = 0, Id = rid.Value.Item1 });
        a.Step(D_ID, new ReadQuorumResponse { CurrentTerm = a.CurrentTerm, CommitIdx = 0, Id = rid.Value.Item1 });
        output = a.GetOutput();
        Assert.That(output.MaxReadIdWithQuorum, Is.Null);

        // Ack in new config as well and see that it is committed now
        a.Step(E_ID, new ReadQuorumResponse { CurrentTerm = a.CurrentTerm, CommitIdx = 0, Id = rid.Value.Item1 });
        output = a.GetOutput();
        Assert.That(output.MaxReadIdWithQuorum, Is.Not.Null);

        // check that read_barrier with lower term does not depose the leader
        a.Step(E_ID, new ReadQuorumRequest {
            CurrentTerm = a.CurrentTerm - 1,
            LeaderCommitIdx = 10,
            Id = rid.Value.Item1
        });
        Assert.That(a.IsLeader, Is.True);

        // check that read_barrier with higher term leads to the leader step down
        a.Step(E_ID, new ReadQuorumRequest {
            CurrentTerm = a.CurrentTerm + 1,
            LeaderCommitIdx = 10,
            Id = rid.Value.Item1
        });
        Assert.That(a.IsLeader, Is.False);

        // create one node cluster
        var log1 = new RaftLog(new SnapshotDescriptor {
            Idx = 0, Config = Messages.ConfigFromIds(A_ID)
        });
        var aa = CreateFollower(A_ID, log1.Clone());
        // Make AA a leader
        ElectionTimeout(aa);
        Assert.That(aa.IsLeader, Is.True);
        aa.GetOutput();

        // execute read barrier
        rid = aa.StartReadBarrier(A_ID);
        Assert.That(rid, Is.Not.Null);

        // check that it completes immediately
        output = aa.GetOutput();
        Assert.That(output.MaxReadIdWithQuorum, Is.Not.Zero);
    }
}
