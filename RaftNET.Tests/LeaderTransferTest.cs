using RaftNET.FailureDetectors;
using RaftNET.Records;

namespace RaftNET.Tests;

public class LeaderTransferTest : FSMTestBase {
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

        fsm2.AddEntry(new Void());

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

    [Test]
    public void TestLeaderTransfereeDiesUponReceivingTimeoutNow() {
        var fd = new DiscreteFailureDetector();
        var log = new Log(new SnapshotDescriptor {
            Idx = 0, Config = Messages.ConfigFromIds(A_ID, B_ID, C_ID, D_ID)
        });
        var a = CreateFollower(A_ID, log.Clone(), fd);
        var b = CreateFollower(B_ID, log.Clone(), fd);
        var c = CreateFollower(C_ID, log.Clone(), fd);
        var d = CreateFollower(D_ID, log.Clone(), fd);

        var map = new Dictionary<ulong, FSM> {
            { A_ID, a },
            { B_ID, b },
            { C_ID, c },
            { D_ID, d },
        };

        ElectionTimeout(a);
        Communicate(a, b, c, d);
        Assert.That(a.IsLeader, Is.True);

        var newCfg = Messages.ConfigFromIds(B_ID, C_ID, D_ID);
        a.AddEntry(newCfg);

        CommunicateUntil(() => !a.IsLeader, a, b, c, d);
        Assert.That(a.IsFollower, Is.True);

        map.Remove(A_ID);

        var output = a.GetOutput();
        Assert.That(output.Messages, Has.Count.EqualTo(1));
        Assert.That(output.Messages.Last().Message.IsTimeoutNowRequest, Is.True);
        var timeoutNowTargetId = output.Messages.Last().To;
        var timeoutNowMessage = output.Messages.Last().Message;

        map[timeoutNowTargetId].Step(A_ID, timeoutNowMessage);

        fd.MarkDead(timeoutNowTargetId);
        map.Remove(timeoutNowTargetId);

        FSM? first = null;
        FSM? second = null;
        var i = 0;
        foreach (var fsm in map.Keys) {
            if (i == 0) {
                first = map[fsm];
                i++;
            } else if (i == 1) {
                second = map[fsm];
                break;
            }
        }
        Assert.Multiple(() => {
            Assert.That(first, Is.Not.Null);
            Assert.That(second, Is.Not.Null);
        });

        ElectionTimeout(first);
        ElectionThreshold(second);

        CommunicateImpl(() => false, map);
        var finalLeader = SelectLeader(b, c, d);
        Assert.That(finalLeader.Id == first.Id || finalLeader.Id == second.Id, Is.True);
    }

    [Test]
    public void TestLeaderTransferLostTimeoutNow() {
        var log = new Log(new SnapshotDescriptor {
            Idx = 0, Config = Messages.ConfigFromIds(A_ID, B_ID, C_ID)
        });
        var a = CreateFollower(A_ID, log.Clone());
        var b = CreateFollower(B_ID, log.Clone());
        var c = CreateFollower(C_ID, log.Clone());

        ElectionTimeout(a);
        Communicate(a, b, c);
        Assert.That(a.IsLeader, Is.True);

        var newCfg = Messages.ConfigFromIds(B_ID, C_ID);
        a.AddEntry(newCfg);

        CommunicateUntil(() => !a.IsLeader, a, b, c);

        var output = a.GetOutput();
        Assert.Multiple(() => {
            Assert.That(output.Messages, Has.Count.EqualTo(1));
            Assert.That(output.Messages.Last().Message.IsTimeoutNowRequest, Is.True);
            // ... and lose it.
            Assert.That(b.IsFollower, Is.True);
            Assert.That(c.IsFollower, Is.True);
        });

        ElectionTimeout(b);
        ElectionThreshold(c);
        Communicate(b, c);
        Assert.That(b.IsLeader, Is.True);
    }

    [Test]
    public void TestLeaderTransferLostForceVoteRequest() {
        var log = new Log(new SnapshotDescriptor {
            Idx = 0, Config = Messages.ConfigFromIds(A_ID, B_ID, C_ID)
        });
        var a = CreateFollower(A_ID, log.Clone());
        var b = CreateFollower(B_ID, log.Clone());
        var c = CreateFollower(C_ID, log.Clone());

        var map = new Dictionary<ulong, FSM> {
            { A_ID, a },
            { B_ID, b },
            { C_ID, c },
        };

        ElectionTimeout(a);
        Communicate(a, b, c);
        Assert.That(a.IsLeader, Is.True);

        var newCfg = Messages.ConfigFromIds(B_ID, C_ID);
        a.AddEntry(newCfg);

        CommunicateUntil(() => !a.IsLeader, a, b, c);
        map.Remove(A_ID);

        var output = a.GetOutput();
        Assert.That(output.Messages, Has.Count.EqualTo(1));
        Assert.That(output.Messages.Last().Message.IsTimeoutNowRequest, Is.True);
        var timeoutNowTargetId = output.Messages.Last().To;
        var timeoutNowMessage = output.Messages.Last().Message;

        var timeoutNowTarget = map[timeoutNowTargetId];
        timeoutNowTarget.Step(A_ID, timeoutNowMessage);
        output = timeoutNowTarget.GetOutput();
        Assert.That(output.Messages, Has.Count.EqualTo(1));
        Assert.That(output.Messages.Last().Message.IsVoteRequest, Is.True);
        var voteRequest1 = output.Messages.First().Message.VoteRequest;
        Assert.That(voteRequest1.Force, Is.True);

        ElectionTimeout(timeoutNowTarget);
        output = timeoutNowTarget.GetOutput();
        Assert.That(output.Messages, Has.Count.EqualTo(1));
        Assert.That(output.Messages.Last().Message.IsVoteRequest, Is.True);
        var voteRequest1Regular = output.Messages.First().Message.VoteRequest;
        var voteRequest1RegularTarget = output.Messages.First().To;
        Assert.That(voteRequest1Regular.Force, Is.False);

        ElectionThreshold(map[voteRequest1RegularTarget]);
        map[voteRequest1RegularTarget].Step(timeoutNowTargetId, voteRequest1Regular);

        Communicate(b, c);
        var finalLeader = SelectLeader(b, c);
        Assert.That(finalLeader.Id, Is.EqualTo(timeoutNowTargetId));
    }
}
