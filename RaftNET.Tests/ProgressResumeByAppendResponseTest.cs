using RaftNET.FailureDetectors;
using RaftNET.Replication;

namespace RaftNET.Tests;

public class ProgressResumeByAppendResponseTest : FSMTestBase {
    [Test]
    public void TestProgressResumeByAppendResponse() {
        var cfg = Messages.ConfigFromIds(Id1, Id2);
        var log = new RaftLog(new SnapshotDescriptor { Config = cfg });
        var fsm = new FSMDebug(Id1, 0, 0, log, new TrivialFailureDetector(), FSMConfig);

        ElectionTimeout(fsm);
        var output = fsm.GetOutput();
        Assert.That(output.TermAndVote, Is.Not.Null);
        fsm.Step(Id2, new VoteResponse {
            CurrentTerm = output.TermAndVote.Term, VoteGranted = true
        });
        Assert.That(fsm.IsLeader);

        var fprogress = fsm.GetProgress(Id2);
        Assert.That(fprogress, Is.Not.Null);
        Assert.That(fprogress.State, Is.EqualTo(FollowerProgressState.Probe));

        var fprogress2 = fsm.GetProgress(Id2);

        output = fsm.GetOutput();
        Assert.That(fprogress2, Is.Not.Null);
        Assert.Multiple(() => {
            Assert.That(fprogress2.ProbeSent, Is.True);
            Assert.That(output.Messages, Has.Count.EqualTo(1));
        });
        var fake = output.Messages.Last().Message.AppendRequest;

        fsm.AddEntry("1");
        output = fsm.GetOutput();
        Assert.That(output.Messages, Is.Empty);

        // ack dummy entry
        fsm.Step(Id2, new AppendResponse {
            CurrentTerm = fake.CurrentTerm,
            CommitIdx = fake.Entries.First().Idx,
            Accepted = new AppendAccepted {
                LastNewIdx = fake.Entries.First().Idx
            }
        });

        // After the ack mode becomes pipeline and sending resumes
        Assert.That(fprogress.State, Is.EqualTo(FollowerProgressState.Pipeline));
        output = fsm.GetOutput();
        Assert.That(output.Messages, Has.Count.EqualTo(1));
    }
}
