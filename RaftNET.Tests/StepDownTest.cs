using System.Text;
using Microsoft.Extensions.Logging;
using RaftNET.FailureDetectors;

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
        var fsm = new FSMDebug(Id1, 1, 0, log,
            new TrivialFailureDetector(), FSMConfig, LoggerFactory.CreateLogger<FSM>());

        // Check that we move to candidate state on timeout_now message
        fsm.Step(Id2, new TimeoutNowRequest { CurrentTerm = fsm.CurrentTerm });
        Assert.That(fsm.IsCandidate, Is.True);
        var output = fsm.GetOutput();
        Assert.That(output.Messages.Last().Message.IsVoteRequest, Is.True);
        var voteRequest = output.Messages.Last().Message.VoteRequest;
        Assert.That(voteRequest.Force, Is.True); // Check that vote_request has `force` flag set.

        // Turn to a leader
        fsm.Step(Id2, new VoteResponse { CurrentTerm = fsm.CurrentTerm, VoteGranted = true });
        Assert.That(fsm.IsLeader, Is.True);

        // make id2's match idx to be up-to-date
        output = fsm.GetOutput();
        Assert.That(output.Messages.Last().Message.IsAppendRequest, Is.True);
        var append = output.Messages.Last().Message.AppendRequest;
        var idx = append.Entries.Last().Idx;
        fsm.Step(Id2, new AppendResponse {
            CurrentTerm = fsm.CurrentTerm, CommitIdx = 0,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });
        Assert.That(fsm.IsLeader, Is.True);

        // start leadership transfer while there is a fully up-to-date follower
        fsm.TransferLeadership();

        // Check that timeout_now message is sent
        output = fsm.GetOutput();
        Assert.That(output.Messages.Count, Is.EqualTo(1));
        Assert.That(output.Messages.Last().Message.IsTimeoutNowRequest, Is.True);

        // Turn to a leader again
        // ... first turn to a follower
        fsm.Step(Id2, new VoteRequest {
            CurrentTerm = fsm.CurrentTerm + 1,
            LastLogIdx = 10, LastLogTerm = 0, IsPreVote = false, Force = true
        });
        Assert.That(fsm.IsFollower, Is.True);
        fsm.GetOutput();
        // ... and now leader
        ElectionTimeout(fsm);
        Assert.That(fsm.IsCandidate, Is.True);
        fsm.GetOutput();
        fsm.Step(Id2, new VoteResponse { CurrentTerm = fsm.CurrentTerm, VoteGranted = true });
        Assert.That(fsm.IsLeader, Is.True);
        output = fsm.GetOutput();
        Assert.That(output.Messages.Last().Message.IsAppendRequest, Is.True);
        append = output.Messages.Last().Message.AppendRequest;
        idx = append.Entries.Last().Idx;

        // start leadership transfer while there is no fully up-to-date follower
        // (dummy entry appended by become_leader is not replicated yet)
        fsm.TransferLeadership();

        // check that no timeout_now message was sent
        output = fsm.GetOutput();
        Assert.That(output.Messages.Count, Is.Zero);

        // Now make non voting follower match the log and see that timeout_now is not sent
        fsm.Step(Id3, new AppendResponse {
            CurrentTerm = fsm.CurrentTerm, CommitIdx = 0,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });
        output = fsm.GetOutput();
        Assert.That(output.Messages.Count, Is.Zero);

        // Now make voting follower match the log and see that timeout_now is sent
        fsm.Step(Id2, new AppendResponse {
            CurrentTerm = fsm.CurrentTerm,
            CommitIdx = 0,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });
        output = fsm.GetOutput();
        Assert.That(output.Messages.Count, Is.EqualTo(1));
        Assert.That(output.Messages.Last().Message.IsTimeoutNowRequest, Is.True);

        // Turn to a leader yet again
        // ... first turn to a follower
        fsm.Step(Id2, new VoteRequest {
            CurrentTerm = fsm.CurrentTerm + 1,
            LastLogIdx = 10, LastLogTerm = 0, IsPreVote = false, Force = true
        });
        Assert.That(fsm.IsFollower, Is.True);
        fsm.GetOutput();
        // ... and now leader
        ElectionTimeout(fsm);
        Assert.That(fsm.IsCandidate, Is.True);
        fsm.GetOutput();
        fsm.Step(Id2, new VoteResponse {
            CurrentTerm = fsm.CurrentTerm,
            VoteGranted = true,
        });
        Assert.That(fsm.IsLeader, Is.True);
        // Commit dummy entry
        output = fsm.GetOutput();
        Assert.That(output.Messages.Last().Message.IsAppendRequest, Is.True);
        append = output.Messages.Last().Message.AppendRequest;
        idx = append.Entries.Last().Idx;
        fsm.Step(Id2, new AppendResponse {
            CurrentTerm = fsm.CurrentTerm,
            CommitIdx = idx,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });

        // Drop the leader from the current config and see that stepdown message is sent
        var newConfig = new Configuration {
            Current = {
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = Id2 }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = Id3 }, CanVote = false }
            }
        };
        fsm.AddEntry(newConfig);
        output = fsm.GetOutput();
        Assert.That(output.Messages.Last().Message.IsAppendRequest, Is.True);
        append = output.Messages.Last().Message.AppendRequest;
        idx = append.Entries.Last().Idx;
        // Accept joint config entry on id2
        fsm.Step(Id2, new AppendResponse {
            CurrentTerm = fsm.CurrentTerm,
            CommitIdx = idx,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });
        // fsm added new config to the log
        output = fsm.GetOutput();
        Assert.That(output.Messages.Last().Message.IsAppendRequest, Is.True);
        append = output.Messages.Last().Message.AppendRequest;
        idx = append.Entries.Last().Idx;
        // Accept new config entry on id2
        fsm.Step(Id2, new AppendResponse {
            CurrentTerm = fsm.CurrentTerm,
            CommitIdx = idx,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });

        // And check that the deposed leader sent timeout_now
        output = fsm.GetOutput();
        Assert.That(output.Messages.Count, Is.EqualTo(1));
        Assert.That(output.Messages.Last().Message.IsTimeoutNowRequest, Is.True);

        // Check that leader stepdown works when the leader is removed from the config
        // and there are entries above C_new in its log
        var cfg2 = Messages.ConfigFromIds(Id1, Id2, Id3);
        var log2 = new Log(new SnapshotDescriptor { Config = cfg2 });
        var fsm2 = new FSMDebug(Id1, 1, 0, log2, new TrivialFailureDetector(), FSMConfig, LoggerFactory.CreateLogger<FSM>());

        ElectionTimeout(fsm2);
        // Turn to a leader
        fsm2.Step(Id2, new VoteResponse {
            CurrentTerm = fsm2.CurrentTerm,
            VoteGranted = true,
        });
        Assert.That(fsm2.IsLeader, Is.True);
        output = fsm2.GetOutput();
        Assert.That(output.Messages.Last().Message.IsAppendRequest, Is.True);
        append = output.Messages.Last().Message.AppendRequest;
        idx = append.Entries.Last().Idx;
        // Accept the dummy on id2
        fsm2.Step(Id2, new AppendResponse {
            CurrentTerm = fsm2.CurrentTerm,
            CommitIdx = idx,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });
        // Accept the dummy on id3
        fsm2.Step(Id3, new AppendResponse {
            CurrentTerm = fsm2.CurrentTerm,
            CommitIdx = idx,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });

        // Drop the leader from the current config and see that stepdown message is sent
        var newConfig2 = Messages.ConfigFromIds(Id2, Id3);
        fsm2.AddEntry(newConfig2);
        output = fsm2.GetOutput();
        Assert.That(output.Messages.Last().Message.IsAppendRequest, Is.True);
        append = output.Messages.Last().Message.AppendRequest;
        idx = append.Entries.Last().Idx;
        // Accept joint config entry on id2
        fsm2.Step(Id2, new AppendResponse {
            CurrentTerm = fsm2.CurrentTerm,
            CommitIdx = idx,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });
        // Accept joint config entry on id3
        fsm2.Step(Id3, new AppendResponse {
            CurrentTerm = fsm2.CurrentTerm,
            CommitIdx = idx,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });
        // fsm added new config entry
        output = fsm2.GetOutput();
        Assert.That(output.Messages.Last().Message.IsAppendRequest, Is.True);
        append = output.Messages.Last().Message.AppendRequest;
        idx = append.Entries.Last().Idx;

        fsm2.AddEntry(Encoding.UTF8.GetBytes("")); // add one more command that will be not replicated yet

        // Accept new config entry on id2
        fsm2.Step(Id2, new AppendResponse {
            CurrentTerm = fsm2.CurrentTerm,
            CommitIdx = idx,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });
        // Accept new config entry on id3
        fsm2.Step(Id3, new AppendResponse {
            CurrentTerm = fsm2.CurrentTerm,
            CommitIdx = idx,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });
        // C_new is now committed
        output = fsm2.GetOutput(); // this sends out the entry submitted after C_new
        Assert.That(output.Messages.Last().Message.IsAppendRequest, Is.True);
        append = output.Messages.Last().Message.AppendRequest;
        idx = append.Entries.Last().Idx;
        // Accept the entry
        fsm2.Step(Id2, new AppendResponse {
            CurrentTerm = fsm2.CurrentTerm,
            CommitIdx = idx,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });
        // And check that the deposed leader sent timeout_now
        output = fsm2.GetOutput();
        Assert.That(output.Messages.Count, Is.EqualTo(1));
        Assert.That(output.Messages.Last().Message.IsTimeoutNowRequest, Is.True);
    }
}