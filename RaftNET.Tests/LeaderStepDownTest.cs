using RaftNET.FailureDetectors;
using RaftNET.Records;

namespace RaftNET.Tests;

public class LeaderStepDownTest : FSMTestBase {
    [Test]
    public void TestLeaderStepDown() {
        var cfg = new Configuration {
            Current = {
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = Id1 }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = Id2 }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = Id3 }, CanVote = false },
            }
        };
        var log = new Log(new SnapshotDescriptor { Config = cfg });
        var fsm = new FSMDebug(Id1, 1, 0, log, new TrivialFailureDetector(), FSMConfig);

        fsm.Step(Id2, new TimeoutNowRequest { CurrentTerm = fsm.CurrentTerm });
        Assert.That(fsm.IsCandidate, Is.True);
        var output = fsm.GetOutput();
        var voteRequest = output.Messages.Last().Message.VoteRequest;
        Assert.That(voteRequest.Force, Is.True);

        fsm.Step(Id2, new VoteResponse { CurrentTerm = fsm.CurrentTerm, VoteGranted = true });
        Assert.That(fsm.IsLeader, Is.True);

        output = fsm.GetOutput();
        var append = output.Messages.Last().Message.AppendRequest;
        var idx = append.Entries.Last().Idx;
        fsm.Step(Id2, new AppendResponse {
            CurrentTerm = fsm.CurrentTerm,
            CommitIdx = 0,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });

        fsm.TransferLeadership();

        output = fsm.GetOutput();
        Assert.That(output.Messages, Has.Count.EqualTo(1));
        Assert.That(output.Messages.Last().Message.IsTimeoutNowRequest, Is.True);

        fsm.Step(Id2, new VoteRequest {
            CurrentTerm = fsm.CurrentTerm + 1,
            LastLogIdx = 10,
            LastLogTerm = 0,
            IsPreVote = false,
            Force = true
        });
        Assert.That(fsm.IsFollower, Is.True);
        fsm.GetOutput();

        ElectionTimeout(fsm);
        Assert.That(fsm.IsCandidate, Is.True);
        output = fsm.GetOutput();
        fsm.Step(Id2, new VoteResponse {
            CurrentTerm = fsm.CurrentTerm,
            VoteGranted = true,
            IsPreVote = false
        });
        Assert.That(fsm.IsLeader, Is.True);
        output = fsm.GetOutput();
        append = output.Messages.Last().Message.AppendRequest;
        idx = append.Entries.Last().Idx;

        fsm.TransferLeadership();

        output = fsm.GetOutput();
        Assert.That(output.Messages, Is.Empty);

        fsm.Step(Id3, new AppendResponse {
            CurrentTerm = fsm.CurrentTerm,
            CommitIdx = 0,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });
        output = fsm.GetOutput();
        Assert.That(output.Messages, Is.Empty);

        fsm.Step(Id2, new AppendResponse {
            CurrentTerm = fsm.CurrentTerm,
            CommitIdx = 0,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });
        output = fsm.GetOutput();
        Assert.That(output.Messages, Has.Count.EqualTo(1));
        Assert.That(output.Messages.Last().Message.IsTimeoutNowRequest, Is.True);

        fsm.Step(Id2, new VoteRequest {
            CurrentTerm = fsm.CurrentTerm + 1,
            LastLogIdx = 10,
            LastLogTerm = 0,
            IsPreVote = false,
            Force = true
        });
        Assert.That(fsm.IsFollower, Is.True);
        fsm.GetOutput();
        ElectionTimeout(fsm);
        Assert.That(fsm.IsCandidate, Is.True);
        output = fsm.GetOutput();
        fsm.Step(Id2, new VoteResponse {
            CurrentTerm = fsm.CurrentTerm,
            VoteGranted = true,
            IsPreVote = false
        });
        Assert.That(fsm.IsLeader, Is.True);
        output = fsm.GetOutput();
        append = output.Messages.Last().Message.AppendRequest;
        idx = append.Entries.Last().Idx;
        fsm.Step(Id2, new AppendResponse {
            CurrentTerm = fsm.CurrentTerm,
            CommitIdx = idx,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });

        var newCfg = new Configuration {
            Current = {
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = Id2 }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = Id3 }, CanVote = false }
            }
        };
        fsm.AddEntry(newCfg);
        output = fsm.GetOutput();
        append = output.Messages.Last().Message.AppendRequest;
        idx = append.Entries.Last().Idx;

        fsm.Step(Id2, new AppendResponse {
            CurrentTerm = fsm.CurrentTerm,
            CommitIdx = idx,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });
        output = fsm.GetOutput();
        append = output.Messages.Last().Message.AppendRequest;
        idx = append.Entries.Last().Idx;
        fsm.Step(Id2, new AppendResponse {
            CurrentTerm = fsm.CurrentTerm,
            CommitIdx = idx,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });

        output = fsm.GetOutput();
        Assert.That(output.Messages, Has.Count.EqualTo(1));
        Assert.That(output.Messages.Last().Message.IsTimeoutNowRequest, Is.True);

        var cfg2 = new Configuration {
            Current = {
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = Id1 }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = Id2 }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = Id3 }, CanVote = true },
            }
        };
        var log2 = new Log(new SnapshotDescriptor { Config = cfg2 });
        var fsm2 = new FSMDebug(Id1, 1, 0, log2, new TrivialFailureDetector(), FSMConfig);

        ElectionTimeout(fsm2);
        fsm2.Step(Id2, new VoteResponse {
            CurrentTerm = fsm2.CurrentTerm,
            VoteGranted = true,
        });
        Assert.That(fsm2.IsLeader, Is.True);
        output = fsm2.GetOutput();
        append = output.Messages.Last().Message.AppendRequest;
        idx = append.Entries.Last().Idx;
        fsm2.Step(Id2, new AppendResponse {
            CurrentTerm = fsm2.CurrentTerm,
            CommitIdx = idx,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });
        fsm2.Step(Id3, new AppendResponse {
            CurrentTerm = fsm2.CurrentTerm,
            CommitIdx = idx,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });

        var newCfg2 = new Configuration {
            Current = {
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = Id2 }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = Id3 }, CanVote = true },
            }
        };
        fsm2.AddEntry(newCfg2);
        output = fsm2.GetOutput();
        append = output.Messages.Last().Message.AppendRequest;
        idx = append.Entries.Last().Idx;

        fsm2.Step(Id2, new AppendResponse {
            CurrentTerm = fsm2.CurrentTerm,
            CommitIdx = idx,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });
        fsm2.Step(Id3, new AppendResponse {
            CurrentTerm = fsm2.CurrentTerm,
            CommitIdx = idx,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });

        output = fsm2.GetOutput();
        append = output.Messages.Last().Message.AppendRequest;
        idx = append.Entries.Last().Idx;

        fsm2.AddEntry(new Dummy());

        fsm2.Step(Id2, new AppendResponse {
            CurrentTerm = fsm2.CurrentTerm,
            CommitIdx = idx,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });
        fsm2.Step(Id3, new AppendResponse {
            CurrentTerm = fsm2.CurrentTerm,
            CommitIdx = idx,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });
        output = fsm2.GetOutput();
        append = output.Messages.Last().Message.AppendRequest;
        idx = append.Entries.Last().Idx;
        fsm2.Step(Id2, new AppendResponse {
            CurrentTerm = fsm2.CurrentTerm,
            CommitIdx = idx,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });
        output = fsm2.GetOutput();
        Assert.That(output.Messages, Has.Count.EqualTo(1));
        Assert.That(output.Messages.Last().Message.IsTimeoutNowRequest, Is.True);
    }
}