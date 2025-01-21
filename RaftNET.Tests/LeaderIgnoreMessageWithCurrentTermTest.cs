using RaftNET.FailureDetectors;

namespace RaftNET.Tests;

public class LeaderIgnoreMessageWithCurrentTermTest : FSMTestBase {
    [Test]
    public void TestLeaderIgnoresMessagesWithCurrentTerm() {
        // Check that the leader properly handles InstallSnapshot/AppendRequest/VoteRequest
        // messages carrying its own term.
        var fd = new DiscreteFailureDetector();
        var log = new RaftLog(new SnapshotDescriptor { Idx = 0, Config = Messages.ConfigFromIds(A_ID, B_ID) });
        var a = CreateFollower(A_ID, log.Clone(), fd);
        var b = CreateFollower(B_ID, log.Clone(), fd);
        ElectionTimeout(a);
        Communicate(a, b);
        Assert.That(a.IsLeader, Is.True);
        // Check that InstallSnapshot with current term gets negative reply
        a.Step(B_ID, new InstallSnapshotRequest { CurrentTerm = a.CurrentTerm });
        var output = a.GetOutput();
        Assert.Multiple(() => {
            Assert.That(output.Messages, Has.Count.EqualTo(1));
            Assert.That(output.Messages.First().Message.IsSnapshotResponse, Is.True);
            Assert.That(output.Messages.First().Message.SnapshotResponse.Success, Is.False);
        });
        // Check that AppendRequest with current term is ignored by the leader
        a.Step(B_ID, new AppendRequest { CurrentTerm = a.CurrentTerm });
        output = a.GetOutput();
        Assert.That(output.Messages, Has.Count.EqualTo(0));
        // Check that VoteRequest with current term is not granted
        a.Step(B_ID, new VoteRequest {
            CurrentTerm = a.CurrentTerm,
            LastLogIdx = 0,
            LastLogTerm = 0,
            IsPreVote = false,
            Force = false
        });
        output = a.GetOutput();
        Assert.Multiple(() => {
            Assert.That(output.Messages, Has.Count.EqualTo(1));
            Assert.That(output.Messages.First().Message.IsVoteResponse, Is.True);
            Assert.That(output.Messages.First().Message.VoteResponse.VoteGranted, Is.False);
        });
    }
}
