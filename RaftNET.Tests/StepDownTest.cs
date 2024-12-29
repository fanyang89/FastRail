using Microsoft.AspNetCore.Identity;

namespace RaftNET.Tests;

public class StepDownTest : FSMTestBase {
    [Test]
    public void StepDownTest1() {
        var cfg = new Configuration {
            Current = {
                new[] {
                    new ConfigMember { ServerAddress = new ServerAddress { ServerId = Id1 }, CanVote = true },
                    new ConfigMember { ServerAddress = new ServerAddress { ServerId = Id2 }, CanVote = true },
                    new ConfigMember { ServerAddress = new ServerAddress { ServerId = Id3 }, CanVote = false },
                }
            }
        };
        var log = new Log(new SnapshotDescriptor { Config = cfg });
        var fsm = new FSMDebug(Id1, 1, 0, log, new TrivialFailureDetector(), FSMConfig);

        fsm.Step(Id2, new TimeoutNowRequest { CurrentTerm = fsm.CurrentTerm });
        Assert.That(fsm.IsCandidate, Is.True);
        var output = fsm.GetOutput();
        Assert.That(output.Messages.Last().Message.IsVoteRequest, Is.True);
        var voteRequest = output.Messages.Last().Message.VoteRequest;
        Assert.That(voteRequest.Force, Is.True);

        fsm.Step(Id2, new VoteResponse { CurrentTerm = fsm.CurrentTerm, VoteGranted = true });
        Assert.That(fsm.IsLeader, Is.True);

        output = fsm.GetOutput();
        Assert.That(output.Messages.Last().Message.IsAppendRequest, Is.True);
        var append = output.Messages.Last().Message.AppendRequest;
        var idx = append.Entries.Last().Idx;
        fsm.Step(Id2, new AppendResponse {
            CurrentTerm = fsm.CurrentTerm, CommitIdx = 0,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });

        fsm.TransferLeadership();

        output = fsm.GetOutput();
        Assert.That(output.Messages.Count, Is.EqualTo(1));
        Assert.That(output.Messages.Last().Message.IsTimeoutNowRequest, Is.True);

        fsm.Step(Id2, new VoteRequest {
            CurrentTerm = fsm.CurrentTerm + 1,
            LastLogIdx = 10, LastLogTerm = 0, IsPreVote = false, Force = true
        });
        Assert.That(fsm.IsFollower, Is.True);
        fsm.GetOutput();
        ElectionTimeout(fsm);
        Assert.That(fsm.IsCandidate, Is.True);
        output = fsm.GetOutput();
        fsm.Step(Id2, new VoteResponse {
            CurrentTerm = fsm.CurrentTerm, VoteGranted = true,
        });
        Assert.That(fsm.IsLeader, Is.True);
        output = fsm.GetOutput();
        Assert.That(output.Messages.Last().Message.IsAppendRequest, Is.True);
        append = output.Messages.Last().Message.AppendRequest;
        idx = append.Entries.Last().Idx;
    }
}