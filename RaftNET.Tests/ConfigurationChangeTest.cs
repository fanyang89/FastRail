using System.Runtime.InteropServices;
using Microsoft.VisualStudio.TestPlatform.Utilities;
using RaftNET.Exceptions;
using RaftNET.FailureDetectors;
using RaftNET.Records;
using RaftNET.Replication;

namespace RaftNET.Tests;

public class ConfigurationChangeTest : FSMTestBase {
    [Test]
    public void AddNode() {
        var cfg = Messages.ConfigFromIds(Id1, Id2);
        var log = new Log(new SnapshotDescriptor { Idx = 100, Config = cfg });
        var fsm = CreateFollower(Id1, log);
        Assert.That(fsm.IsFollower, Is.True);

        ElectionTimeout(fsm);
        Assert.That(fsm.IsCandidate, Is.True);
        var output = fsm.GetOutput();
        Assert.That(output.TermAndVote, Is.Not.Null);
        fsm.Step(Id2, new VoteResponse { CurrentTerm = output.TermAndVote.Term, VoteGranted = true });
        Assert.That(fsm.IsLeader, Is.True);

        // A new leader applies one fake entry
        output = fsm.GetOutput();
        Assert.That(output.LogEntries.Count, Is.EqualTo(1));
        Assert.That(output.LogEntries.First().Dummy, Is.Not.Null);
        Assert.That(output.Committed.Count, Is.Zero);
        // accept fake entry, otherwise no more entries will be sent
        Assert.That(output.Messages.Count, Is.EqualTo(1));
        var msg = output.Messages.Last().Message.AppendRequest;
        var idx = msg.Entries.Last().Idx;
        fsm.Step(Id2,
            new AppendResponse {
                CurrentTerm = msg.CurrentTerm, CommitIdx = idx, Accepted = new AppendAccepted { LastNewIdx = idx }
            });

        var newConfig = Messages.ConfigFromIds(Id1, Id2, Id3);
        fsm.AddEntry(newConfig);
        Assert.Throws<ConfigurationChangeInProgressException>(() => fsm.AddEntry(newConfig));
        Assert.That(fsm.GetConfiguration().IsJoint, Is.True);
        Assert.That(fsm.GetConfiguration().Current.Count, Is.EqualTo(3));
        Assert.That(fsm.GetConfiguration().Previous.Count, Is.EqualTo(2));
        output = fsm.GetOutput();
        Assert.That(output.LogEntries.Count, Is.EqualTo(1));
        Assert.That(output.Messages.Count, Is.EqualTo(2));
        msg = output.Messages.Last().Message.AppendRequest;
        idx = msg.Entries.Last().Idx;

        fsm.Step(Id2,
            new AppendResponse {
                CurrentTerm = msg.CurrentTerm, CommitIdx = idx, Accepted = new AppendAccepted { LastNewIdx = idx }
            });
        Assert.That(fsm.GetConfiguration().IsJoint, Is.False);
        Assert.Throws<ConfigurationChangeInProgressException>(() => fsm.AddEntry(newConfig));
        output = fsm.GetOutput();
        Assert.That(output.LogEntries.Count, Is.EqualTo(1));
        Assert.That(output.Messages.Count, Is.GreaterThanOrEqualTo(1));

        msg = output.Messages.Last().Message.AppendRequest;
        idx = msg.Entries.Last().Idx;
        fsm.Step(Id2,
            new AppendResponse {
                CurrentTerm = msg.CurrentTerm, CommitIdx = idx, Accepted = new AppendAccepted { LastNewIdx = idx }
            });
        Assert.That(fsm.GetConfiguration().Current.Count, Is.EqualTo(3));
        fsm.Step(Id3,
            new AppendResponse {
                CurrentTerm = msg.CurrentTerm, CommitIdx = idx, Accepted = new AppendAccepted { LastNewIdx = idx }
            });

        // Check that we can start a new confchange
        fsm.AddEntry(Messages.ConfigFromIds(Id1, Id2));
    }

    [Test]
    public void RemoveNode() {
        var cfg = Messages.ConfigFromIds(Id1, Id2, Id3);
        var log = new Log(new SnapshotDescriptor { Idx = 100, Config = cfg });
        var fsm = CreateFollower(Id1, log);
        Assert.That(fsm.IsFollower, Is.True);
        ElectionTimeout(fsm);
        Assert.That(fsm.IsCandidate, Is.True);
        var output = fsm.GetOutput();
        Assert.That(output.Messages.Count, Is.EqualTo(2));

        if (output.Messages.Count > 0) {
            Assert.That(output.Messages[0].Message.IsVoteRequest, Is.True);
        }

        if (output.Messages.Count > 1) {
            Assert.That(output.Messages[1].Message.IsVoteRequest, Is.True);
        }

        Assert.That(output.TermAndVote, Is.Not.Null);
        fsm.Step(Id2, new VoteResponse { CurrentTerm = output.TermAndVote.Term, VoteGranted = true });
        Assert.That(fsm.IsLeader, Is.True);
        output = fsm.GetOutput();
        Assert.That(output.LogEntries.Count, Is.EqualTo(1));
        Assert.That(output.LogEntries.First().Dummy, Is.Not.Null);
        Assert.That(output.Messages.Count, Is.EqualTo(2));
        var msg = output.Messages.Last().Message.AppendRequest;
        var idx = msg.Entries.Last().Idx;
        fsm.Step(Id2,
            new AppendResponse {
                CurrentTerm = msg.CurrentTerm, CommitIdx = idx, Accepted = new AppendAccepted { LastNewIdx = idx }
            });
        fsm.Step(Id3,
            new AppendResponse {
                CurrentTerm = msg.CurrentTerm, CommitIdx = idx, Accepted = new AppendAccepted { LastNewIdx = idx }
            });

        var newConfig = Messages.ConfigFromIds(Id1, Id2);
        fsm.AddEntry(newConfig);
        Assert.That(fsm.GetConfiguration().IsJoint, Is.True);
        Assert.That(fsm.GetConfiguration().Current.Count, Is.EqualTo(2));
        Assert.That(fsm.GetConfiguration().Previous.Count, Is.EqualTo(3));
        output = fsm.GetOutput();
        Assert.That(output.LogEntries.Count, Is.EqualTo(1));
        Assert.That(output.LogEntries.First().Configuration, Is.Not.Null);
        Assert.That(output.Messages.Count, Is.EqualTo(2));
        Assert.That(output.Messages.First().Message.IsAppendRequest, Is.True);
        msg = output.Messages.First().Message.AppendRequest;
        Assert.That(msg.Entries.Count, Is.EqualTo(1));
        Assert.That(msg.Entries.First().DataCase, Is.EqualTo(LogEntry.DataOneofCase.Configuration));
        idx = msg.Entries.Last().Idx;
        Assert.That(idx, Is.EqualTo(102));
        fsm.Step(Id2,
            new AppendResponse {
                CurrentTerm = msg.CurrentTerm, CommitIdx = idx, Accepted = new AppendAccepted { LastNewIdx = idx }
            });
        output = fsm.GetOutput();
        Assert.That(output.Messages.Count, Is.EqualTo(1));
        msg = output.Messages.First().Message.AppendRequest;
        Assert.That(output.LogEntries.First().DataCase, Is.EqualTo(LogEntry.DataOneofCase.Configuration));
        idx = msg.Entries.Last().Idx;
        Assert.That(idx, Is.EqualTo(103));
        Assert.That(fsm.GetConfiguration().Current.Count, Is.EqualTo(2));
        Assert.That(fsm.GetConfiguration().IsJoint, Is.False);
        fsm.Step(Id2,
            new AppendResponse {
                CurrentTerm = msg.CurrentTerm, CommitIdx = idx, Accepted = new AppendAccepted { LastNewIdx = idx }
            });
        var newConfig2 = Messages.ConfigFromIds(Id1, Id2, Id3);
        fsm.AddEntry(newConfig2);
        Assert.That(fsm.GetConfiguration().Current.Count, Is.EqualTo(3));
    }

    [Test]
    public void ReplaceNode() {
        var cfg = Messages.ConfigFromIds(Id1, Id2, Id3);
        var log = new Log(new SnapshotDescriptor { Idx = 100, Config = cfg });
        var fsm = CreateFollower(Id1, log);
        Assert.That(fsm.IsFollower, Is.True);
        ElectionTimeout(fsm);
        Assert.That(fsm.IsCandidate, Is.True);
        var output = fsm.GetOutput();
        Assert.That(output.TermAndVote, Is.Not.Null);
        fsm.Step(Id2, new VoteResponse { CurrentTerm = output.TermAndVote.Term, VoteGranted = true });
        Assert.That(fsm.IsLeader, Is.True);
        output = fsm.GetOutput();
        Assert.That(output.LogEntries.Count, Is.EqualTo(1));
        Assert.That(output.LogEntries.First().Dummy, Is.Not.Null);
        Assert.That(output.Committed.Count, Is.EqualTo(0));
        Assert.That(output.Messages.Count, Is.EqualTo(2));
        var msg = output.Messages.Last().Message.AppendRequest;
        var idx = msg.Entries.Last().Idx;
        fsm.Step(Id2,
            new AppendResponse {
                CurrentTerm = msg.CurrentTerm, CommitIdx = idx, Accepted = new AppendAccepted { LastNewIdx = idx }
            });
        fsm.Step(Id3,
            new AppendResponse {
                CurrentTerm = msg.CurrentTerm, CommitIdx = idx, Accepted = new AppendAccepted { LastNewIdx = idx }
            });

        var newConfig = Messages.ConfigFromIds(Id1, Id2, Id4);
        fsm.AddEntry(newConfig);
        Assert.That(fsm.GetConfiguration().IsJoint, Is.True);
        Assert.That(fsm.GetConfiguration().Current.Count, Is.EqualTo(3));
        Assert.That(fsm.GetConfiguration().Previous.Count, Is.EqualTo(3));
        output = fsm.GetOutput();
        Assert.That(output.Messages.First().Message.IsAppendRequest, Is.True);
        msg = output.Messages.First().Message.AppendRequest;
        idx = msg.Entries.Last().Idx;
        fsm.Step(Id2,
            new AppendResponse {
                CurrentTerm = msg.CurrentTerm, CommitIdx = idx, Accepted = new AppendAccepted { LastNewIdx = idx }
            });
        Assert.That(fsm.GetConfiguration().IsJoint, Is.False);
        output = fsm.GetOutput();
        Assert.That(output.LogEntries.Count, Is.EqualTo(1));
        Assert.That(output.LogEntries.First().DataCase, Is.EqualTo(LogEntry.DataOneofCase.Configuration));
        Assert.That(output.Messages.Count, Is.GreaterThanOrEqualTo(1));
        msg = output.Messages.Last().Message.AppendRequest;
        idx = msg.Entries.Last().Idx;
        fsm.Step(Id2,
            new AppendResponse {
                CurrentTerm = msg.CurrentTerm, CommitIdx = idx, Accepted = new AppendAccepted { LastNewIdx = idx }
            });
        Assert.That(fsm.GetConfiguration().Current.Count, Is.EqualTo(3));
        Assert.That(fsm.GetConfiguration().IsJoint, Is.False);
    }

    [Test]
    public void Replace1() {
        // Configuration change {A, B} to {C, D}
        // Similar to A -> B change, but with many nodes,
        // so C_new has to campaign after configuration change.
        var log = new Log(new SnapshotDescriptor {
            Idx = 0, Config = Messages.ConfigFromIds(A_ID, B_ID)
        });
        var a = CreateFollower(A_ID, log.Clone());
        var b = CreateFollower(B_ID, log.Clone());
        ElectionTimeout(a);
        Communicate(a, b);
        Assert.That(a.IsLeader, Is.True);

        var c = CreateFollower(C_ID, log.Clone());
        var d = CreateFollower(D_ID, log.Clone());

        a.AddEntry(Messages.ConfigFromIds(C_ID, D_ID));
        Communicate(a, b, c, d);

        Assert.Multiple(() => {
            Assert.That(a.CurrentTerm, Is.EqualTo(1));
            Assert.That(a.IsFollower, Is.True);
            Assert.That(b.IsFollower, Is.True);
        });

        ElectionTimeout(c);
        ElectionThreshold(d);
        Communicate(a, b, c, d);
        Assert.Multiple(() => {
            Assert.That(c.CurrentTerm, Is.EqualTo(2));
            Assert.That(c.IsLeader, Is.True);
            Assert.That(c.GetConfiguration().IsJoint, Is.False);
            Assert.That(c.GetConfiguration().Current, Has.Count.EqualTo(2));
        });
    }

    [Test]
    public void Replace2() {
        // Configuration change {A, B, C} to {C, D, E}
        // Check configuration changes when C_old and C_new have no common quorum,
        // test leader change during configuration change
        var fd = new DiscreteFailureDetector();
        var log = new Log(new SnapshotDescriptor {
            Idx = 0, Config = Messages.ConfigFromIds(A_ID, B_ID, C_ID)
        });
        var a = CreateFollower(A_ID, log.Clone());
        var b = CreateFollower(B_ID, log.Clone(), fd);
        var c = CreateFollower(C_ID, log.Clone(), fd);
        ElectionTimeout(a);
        Communicate(a, b, c);
        Assert.That(a.IsLeader, Is.True);

        var d = CreateFollower(D_ID, log.Clone());
        var e = CreateFollower(E_ID, log.Clone());

        a.AddEntry(Messages.ConfigFromIds(C_ID, D_ID, E_ID));
        Communicate(a, b, c);

        fd.MarkDead(A_ID);
        ElectionTimeout(c);
        Assert.That(c.IsCandidate, Is.True);

        ElectionThreshold(b);
        Communicate(b, c, d, e);
        Assert.Multiple(() => {
            Assert.That(a.IsLeader, Is.True);
            Assert.That(a.CurrentTerm, Is.EqualTo(1));
            Assert.That(b.IsFollower, Is.True);
            Assert.That(c.IsLeader, Is.True);
            Assert.That(d.IsFollower, Is.True);
            Assert.That(e.IsFollower, Is.True);
            Assert.That(c.CurrentTerm, Is.GreaterThanOrEqualTo(2));
            Assert.That(c.GetConfiguration().IsJoint(), Is.False);
            Assert.That(c.GetConfiguration().Current, Has.Count.EqualTo(3));
        });
    }

    [Test]
    public void Replace3() {
        // Configuration change {A, B, C, D, E, F} to {A, B, C, G, H}
        // Test configuration changes in presence of down nodes in C_old
        var fd = new DiscreteFailureDetector();
        var log = new Log(new SnapshotDescriptor {
            Idx = 0, Config = Messages.ConfigFromIds(A_ID, B_ID, C_ID, D_ID, E_ID, F_ID)
        });
        var a = CreateFollower(A_ID, log.Clone(), fd);
        var b = CreateFollower(B_ID, log.Clone(), fd);
        var c = CreateFollower(C_ID, log.Clone(), fd);
        var d = CreateFollower(D_ID, log.Clone(), fd);
        var e = CreateFollower(E_ID, log.Clone(), fd);
        var f = CreateFollower(F_ID, log.Clone(), fd);
        ElectionTimeout(d);
        Communicate(a, d, e, f);
        Assert.That(d.IsLeader, Is.True);

        var g = CreateFollower(G_ID, log.Clone());
        var h = CreateFollower(H_ID, log.Clone());

        d.AddEntry(Messages.ConfigFromIds(A_ID, B_ID, C_ID, G_ID, H_ID));
        Communicate(b, c, d, g, h);
        Assert.Multiple(() => {
            Assert.That(d.IsLeader, Is.True);
            Assert.That(d.GetConfiguration().IsJoint(), Is.True);
        });
        d.Tick();
        Communicate(b, c, e, d, g, h);
        Assert.That(d.IsFollower, Is.True);

        var leader = SelectLeader(a, b, c, g, h);
        Assert.Multiple(() => {
            Assert.That(leader.GetConfiguration().IsJoint(), Is.False);
            Assert.That(leader.GetConfiguration().Current, Has.Count.EqualTo(5));
        });

        fd.MarkAllDead();
        ElectionTimeout(d);
        ElectionTimeout(a);
        Communicate(a, b, c, d, e, f, g, h);
        Assert.That(leader.IsLeader, Is.True);
    }

    [Test]
    public void Replace4() {
        // Check configuration changes work fine with many nodes down
        var fd = new DiscreteFailureDetector();

        var log = new Log(new SnapshotDescriptor {
            Idx = 0, Config = Messages.ConfigFromIds(A_ID, B_ID, C_ID, D_ID, E_ID)
        });
        var a = CreateFollower(A_ID, log.Clone(), fd);
        var b = CreateFollower(B_ID, log.Clone(), fd);
        var c = CreateFollower(C_ID, log.Clone(), fd);
        var d = CreateFollower(D_ID, log.Clone(), fd);
        var e = CreateFollower(E_ID, log.Clone(), fd);
        ElectionTimeout(a);
        Communicate(a, d, e);
        Assert.That(a.IsLeader, Is.True);

        var f = CreateFollower(F_ID, log.Clone());
        var g = CreateFollower(G_ID, log.Clone());

        // Wrap configuration entry into some traffic
        a.AddEntry(new Dummy());
        a.AddEntry(Messages.ConfigFromIds(A_ID, B_ID, C_ID, D_ID, E_ID, F_ID, G_ID));
        a.AddEntry(new Dummy());
        // Without tick() A won't re-try communication with nodes it
        // believes are down (B, C).
        a.Tick();
        // 4 is enough to transition to the new configuration
        Communicate(a, b, c, g);

        Assert.Multiple(() => {
            Assert.That(a.IsLeader, Is.True);
            Assert.That(a.GetConfiguration().IsJoint, Is.False);
            Assert.That(a.GetConfiguration().Current, Has.Count.EqualTo(7));
        });

        a.Tick();
        Communicate(a, b, c, d, e, f, g);

        Assert.Multiple(() => {
            Assert.That(a.LogLastIdx, Is.EqualTo(b.LogLastIdx));
            Assert.That(a.LogLastIdx, Is.EqualTo(c.LogLastIdx));
            Assert.That(a.LogLastIdx, Is.EqualTo(d.LogLastIdx));
            Assert.That(a.LogLastIdx, Is.EqualTo(e.LogLastIdx));
            Assert.That(a.LogLastIdx, Is.EqualTo(f.LogLastIdx));
            Assert.That(a.LogLastIdx, Is.EqualTo(g.LogLastIdx));
            Assert.That(a.IsLeader, Is.True);
            Assert.That(a.GetConfiguration().IsJoint, Is.False);
            Assert.That(a.GetConfiguration().Current, Has.Count.EqualTo(7));
        });
    }

    [Test]
    public void ElectionDuringConfigurationChange() {
        // Joint config has reached old majority, the leader is from a new majority
        var log = new Log(new SnapshotDescriptor { Idx = 0, Config = Messages.ConfigFromIds(A_ID, B_ID, C_ID) });
        var fd = new DiscreteFailureDetector();
        var a = CreateFollower(A_ID, log.Clone(), fd);
        var b = CreateFollower(B_ID, log.Clone(), fd);
        var c = CreateFollower(C_ID, log.Clone(), fd);
        ElectionTimeout(a);
        Communicate(a, b, c);
        a.AddEntry(Messages.ConfigFromIds(C_ID, D_ID, E_ID));
        Communicate(a, b, c);
        fd.MarkDead(A_ID);
        var d = CreateFollower(D_ID, log.Clone(), fd);
        var e = CreateFollower(E_ID, log.Clone(), fd);
        ElectionTimeout(c);
        ElectionThreshold(b);
        CommunicateUntil(() => c.IsLeader, b, c, d, e);
        Assert.That(c.GetConfiguration().IsJoint, Is.True);
        fd.MarkAlive(A_ID);
        Communicate(d, a, b, e);
        fd.MarkAlive(C_ID);
        CommunicateUntil(() => !c.GetConfiguration().IsJoint(), b, c, d, e);
        Communicate(c, d);
        fd.MarkDead(C_ID);
        ElectionTimeout(d);
        // E may still be in joint. It must vote for D anyway. D is in C_new
        // and will replicate C_new to E after becoming a leader
        ElectionThreshold(e);
        a.Tick();
        Communicate(a, d, e);
        Assert.Multiple(() => {
            Assert.That(d.IsLeader, Is.True);
            Assert.That(a.IsFollower, Is.True);
            Assert.That(d.GetConfiguration().IsJoint(), Is.False);
            Assert.That(d.GetConfiguration().Current, Has.Count.EqualTo(3));
        });
    }

    [Test]
    public void TestReplyFromRemovedFollower() {
        // Messages from followers may be delayed.
        // Check they don't upset the leader when they are delivered past configuration change
        var log = new Log(new SnapshotDescriptor { Idx = 0, Config = Messages.ConfigFromIds(A_ID, B_ID) });
        var a = CreateFollower(A_ID, log.Clone());
        var b = CreateFollower(B_ID, log.Clone());
        ElectionTimeout(a);
        Communicate(a, b);
        a.AddEntry(Messages.ConfigFromIds(A_ID));
        Communicate(a, b);
        Assert.Multiple(() => {
            Assert.That(a.IsLeader, Is.True);
            Assert.That(a.GetConfiguration().IsJoint(), Is.False);
            Assert.That(a.GetConfiguration().Current, Has.Count.EqualTo(1));
        });
        var idx = a.LogLastIdx;
        a.Step(B_ID, new AppendResponse {
            CurrentTerm = a.CurrentTerm, CommitIdx = idx, Accepted = new AppendAccepted { LastNewIdx = idx }
        });
        a.Step(B_ID, new AppendResponse {
            CurrentTerm = a.CurrentTerm, CommitIdx = idx, Rejected = new AppendRejected { NonMatchingIdx = idx }
        });
        a.Step(B_ID, new SnapshotResponse { CurrentTerm = a.CurrentTerm, Success = true });
        Assert.That(a.IsLeader, Is.True);
    }

    [Test]
    public void TestLeaderIgnoresMessagesWithCurrentTerm() {
        // Check that the leader properly handles InstallSnapshot/AppendRequest/VoteRequest
        // messages carrying its own term.
        var fd = new DiscreteFailureDetector();
        var log = new Log(new SnapshotDescriptor { Idx = 0, Config = Messages.ConfigFromIds(A_ID, B_ID) });
        var a = CreateFollower(A_ID, log.Clone(), fd);
        var b = CreateFollower(B_ID, log.Clone(), fd);
        ElectionTimeout(a);
        Communicate(a, b);
        Assert.That(a.IsLeader, Is.True);
        // Check that InstallSnapshot with current term gets negative reply
        a.Step(B_ID, new InstallSnapshot { CurrentTerm = a.CurrentTerm });
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

    [Test]
    public void TestReorderedReject() {
        var fsm1 = new FSMDebug(Id1, 1, 0, new Log(new SnapshotDescriptor { Config = Messages.ConfigFromIds(Id1) }),
            new TrivialFailureDetector(), FSMConfig);

        while (!fsm1.IsLeader) {
            fsm1.Tick();
        }
        fsm1.AddEntry(new Dummy());
        fsm1.GetOutput();

        var fsm2 = new FSMDebug(Id2, 1, 0, new Log(new SnapshotDescriptor { Config = new Configuration() }),
            new TrivialFailureDetector(), FSMConfig);

        var routes = new Dictionary<ulong, FSM> {
            { fsm1.Id, fsm1 },
            { fsm2.Id, fsm2 }
        };
        fsm1.AddEntry(Messages.ConfigFromIds(fsm1.Id, fsm2.Id));
        fsm1.Tick();

        // fsm1 sends append_entries with idx=2 to fsm2
        var appendIdx21 = fsm1.GetOutput();
        fsm1.Tick();

        // fsm1 sends append_entries with idx=2 to fsm2 (again)
        var appendIdx22 = fsm1.GetOutput();
        Deliver(routes, fsm1.Id, appendIdx21.Messages);

        // fsm2 rejects the first idx=2 append
        var reject1 = fsm2.GetOutput();
        Deliver(routes, fsm1.Id, appendIdx22.Messages);

        // fsm2 rejects the second idx=2 append
        var reject2 = fsm2.GetOutput();
        Deliver(routes, fsm2.Id, reject1.Messages);

        // fsm1 sends append_entries with idx=1 to fsm2
        var appendIdx1 = fsm1.GetOutput();
        Deliver(routes, fsm1.Id, appendIdx1.Messages);

        // fsm2 accepts the idx=1 append
        var accept = fsm2.GetOutput();
        Deliver(routes, fsm2.Id, accept.Messages);
        Deliver(routes, fsm2.Id, reject2.Messages);
    }

    [Test]
    public void TestNonVoterStaysPipeline() {
        // Check that a node stays in PIPELINE mode through configuration changes
        var cfg = new Configuration {
            Current = {
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = A_ID }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = B_ID }, CanVote = false }
            }
        };
        var log = new Log(new SnapshotDescriptor { Idx = 0, Config = cfg });
        var a = CreateFollower(A_ID, log.Clone());
        var b = CreateFollower(B_ID, log.Clone());
        ElectionTimeout(a);
        Communicate(a);
        Assert.That(a.IsLeader, Is.True);
        var progress = a.GetProgress(B_ID);
        Assert.That(progress, Is.Not.Null);
        Assert.That(progress.State, Is.EqualTo(FollowerProgressState.Probe));
        a.AddEntry(new Dummy());
        a.Tick();
        Communicate(a, b);
        progress = a.GetProgress(B_ID);
        Assert.That(progress, Is.Not.Null);
        Assert.That(progress.State, Is.EqualTo(FollowerProgressState.Pipeline));
        var newCfg = Messages.ConfigFromIds(A_ID, B_ID);
        a.AddEntry(newCfg);
        Communicate(a, b);

        Assert.That(a.GetConfiguration().IsJoint, Is.False);
        Assert.That(a.GetConfiguration().Current.First(x => x.ServerAddress.ServerId == B_ID).CanVote, Is.True);

        progress = a.GetProgress(B_ID);
        Assert.That(progress, Is.Not.Null);
        Assert.That(progress.State, Is.EqualTo(FollowerProgressState.Pipeline));
        a.AddEntry(cfg);

        CommunicateUntil(() => {
            if (RollDice()) {
                a.Tick();
                b.Tick();
            }
            return false;
        }, a, b);
        Assert.That(a.GetConfiguration().IsJoint, Is.False);
        Assert.That(a.GetConfiguration().Current.First(x => x.ServerAddress.ServerId == B_ID).CanVote, Is.False);

        progress = a.GetProgress(B_ID);
        Assert.That(progress, Is.Not.Null);
        Assert.That(progress.State, Is.EqualTo(FollowerProgressState.Pipeline));
    }

    [Test]
    public void TestLeaderChangeToNonVoter() {
        var cfg = new Configuration {
            Current = {
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = A_ID }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = B_ID }, CanVote = false },
            }
        };
        var log = new Log(new SnapshotDescriptor { Idx = 0, Config = cfg });
        var a = CreateFollower(A_ID, log.Clone());
        var b = CreateFollower(B_ID, log.Clone());
        ElectionTimeout(a);
        Communicate(a, b);
        Assert.That(a.IsLeader, Is.True);
        var newCfg = new Configuration {
            Current = {
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = A_ID }, CanVote = false },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = B_ID }, CanVote = true },
            }
        };
        a.AddEntry(newCfg);
        a.Tick();
        Communicate(a, b);
        Assert.Multiple(() => {
            Assert.That(a.IsFollower, Is.True);
            Assert.That(b.IsLeader, Is.True);
        });
        newCfg = new Configuration {
            Current = {
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = A_ID }, CanVote = false },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = B_ID }, CanVote = false },
            }
        };
        Assert.Throws<ArgumentException>(() => b.AddEntry(newCfg));
        newCfg = new Configuration {
            Current = {
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = B_ID }, CanVote = false },
            }
        };
        Assert.Throws<ArgumentException>(() => b.AddEntry(newCfg));
    }

    [Test]
    public void TestNonVoterGetTimeoutNow() {
        var cfg = new Configuration {
            Current = {
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = A_ID }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = B_ID }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = C_ID }, CanVote = false },
            }
        };
        var log = new Log(new SnapshotDescriptor { Idx = 0, Config = cfg });
        var a = CreateFollower(A_ID, log.Clone());
        var b = CreateFollower(B_ID, log.Clone());
        var c = CreateFollower(C_ID, log.Clone());
        ElectionTimeout(a);
        Communicate(a, b, c);
        Assert.That(a.IsLeader, Is.True);
        c.Step(A_ID, new TimeoutNowRequest {
            CurrentTerm = a.CurrentTerm
        });
        c.Tick();
        var output = c.GetOutput();
        Assert.Multiple(() => {
            Assert.That(c.IsFollower, Is.True);
            Assert.That(output.Messages, Is.Empty);
            Assert.That(output.TermAndVote, Is.Null);
        });
        a.AddEntry(new Dummy());
        Communicate(a, b, c);
        Assert.Multiple(() => {
            Assert.That(a.LogLastIdx, Is.EqualTo(c.LogLastIdx));
            Assert.That(a.CurrentTerm, Is.EqualTo(c.CurrentTerm));
            Assert.That(a.IsLeader, Is.True);
        });
    }

    [Test]
    public void TestNonVoterElectionTimeout() {
        var fd = new DiscreteFailureDetector();
        var cfg = new Configuration {
            Current = {
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = A_ID }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = B_ID }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = C_ID }, CanVote = false },
            }
        };
        var log = new Log(new SnapshotDescriptor { Idx = 0, Config = cfg });

        var a = CreateFollower(A_ID, log.Clone(), fd);
        var b = CreateFollower(B_ID, log.Clone(), fd);
        var c = CreateFollower(C_ID, log.Clone(), fd);

        ElectionTimeout(a);
        Communicate(a, b, c);
        Assert.That(a.IsLeader, Is.True);

        fd.MarkAllDead();
        var cTerm = c.CurrentTerm;
        ElectionTimeout(c);
        Assert.Multiple(() => {
            Assert.That(c.IsFollower, Is.True);
            Assert.That(cTerm, Is.EqualTo(c.CurrentTerm));
        });
    }

    [Test]
    public void TestNonVoterVoterLoop() {
        // Test voter-non-voter change in a loop
        var cfg = Messages.ConfigFromIds(A_ID, B_ID, C_ID);
        var cfgWithNonVoter = new Configuration {
            Current = {
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = A_ID }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = B_ID }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = C_ID }, CanVote = false },
            }
        };

        var log = new Log(new SnapshotDescriptor { Idx = 0, Config = cfgWithNonVoter });
        var a = CreateFollower(A_ID, log.Clone());
        var b = CreateFollower(B_ID, log.Clone());
        var c = CreateFollower(C_ID, log.Clone());
        ElectionTimeout(a);
        Communicate(a, b, c);
        Assert.That(a.IsLeader, Is.True);

        for (var i = 0; i < 100; i++) {
            a.AddEntry(i % 2 == 1 ? cfgWithNonVoter : cfg);
            if (RollDice()) {
                a.AddEntry(new Dummy());
            }
            Communicate(a, b, c);
            if (RollDice()) {
                a.AddEntry(new Dummy());
                Communicate(a, b, c);
            }
            if (RollDice(1.0f / 1000)) {
                a.Log.ApplySnapshot(Messages.LogSnapshot(a.Log, a.LogLastIdx), 0, 0);
            }
            if (RollDice(1.0f / 100)) {
                b.Log.ApplySnapshot(Messages.LogSnapshot(a.Log, b.LogLastIdx), 0, 0);
            }
            if (RollDice(1.0f / 5000)) {
                c.Log.ApplySnapshot(Messages.LogSnapshot(a.Log, b.LogLastIdx), 0, 0);
            }
        }

        Assert.Multiple(() => {
            Assert.That(a.IsLeader, Is.True);
            Assert.That(a.CurrentTerm, Is.EqualTo(c.CurrentTerm));
            Assert.That(a.LogLastIdx, Is.EqualTo(c.LogLastIdx));
        });
    }

    [Test]
    public void TestNonVoterConfigurationChangeInSnapshot() {
        var fd = new DiscreteFailureDetector();
        var cfg = Messages.ConfigFromIds(A_ID, B_ID, C_ID);
        var log = new Log(new SnapshotDescriptor { Idx = 0, Config = cfg });
        var a = CreateFollower(A_ID, log.Clone(), fd);
        var b = CreateFollower(B_ID, log.Clone(), fd);
        var c = CreateFollower(C_ID, log.Clone(), fd);
        ElectionTimeout(a);
        Communicate(a, b, c);
        Assert.That(a.IsLeader, Is.True);

        a.AddEntry(new Dummy());
        var cfgWithNonVoter = new Configuration {
            Current = {
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = A_ID }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = B_ID }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = C_ID }, CanVote = false },
            }
        };
        a.Tick();
        a.AddEntry(cfgWithNonVoter);
        a.Tick();

        Communicate(a, b);
        Assert.That(a.GetConfiguration().IsJoint, Is.False);
        Assert.That(a.GetConfiguration().Current.First(x => x.ServerAddress.ServerId == C_ID).CanVote, Is.False);
        a.Tick();
        var aSnp = new SnapshotDescriptor {
            Idx = a.LogLastIdx,
            Term = a.LogLastTerm,
            Config = a.GetConfiguration()
        };
        a.ApplySnapshot(aSnp, 0, 0, true);
        a.Tick();
        Communicate(a, b, c);
        Assert.Multiple(() => {
            Assert.That(a.IsLeader, Is.True);
            Assert.That(a.CurrentTerm, Is.EqualTo(c.CurrentTerm));
            Assert.That(a.LogLastIdx, Is.EqualTo(c.LogLastIdx));
        });

        fd.MarkAllDead();
        ElectionTimeout(c);
        Assert.That(c.IsFollower, Is.True);

        fd.MarkAllAlive();
        a.Tick();
        for (int i = 0; i < 100; i++) {
            a.AddEntry(new Dummy());
        }
        a.AddEntry(cfg);
        a.Tick();

        Communicate(a, b);
        Assert.That(a.GetConfiguration().IsJoint, Is.False);
        Assert.That(a.GetConfiguration().Current.First(x => x.ServerAddress.ServerId == C_ID).CanVote, Is.True);
        a.Tick();
        aSnp = new SnapshotDescriptor {
            Idx = a.LogLastIdx,
            Term = a.LogLastTerm,
            Config = a.GetConfiguration()
        };
        a.ApplySnapshot(aSnp, 0, 0, true);
        a.Tick();
        Communicate(a, b, c);
        Assert.Multiple(() => {
            Assert.That(a.IsLeader, Is.True);
            Assert.That(a.CurrentTerm, Is.EqualTo(c.CurrentTerm));
            Assert.That(a.LogLastIdx, Is.EqualTo(c.LogLastIdx));
        });
        fd.MarkAllDead();
        ElectionTimeout(c);
        Assert.That(c.IsCandidate, Is.True);
        ElectionThreshold(b);
        Communicate(c, b);
        Assert.That(c.IsLeader, Is.True);
    }
}
