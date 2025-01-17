using RaftNET.Exceptions;
using RaftNET.FailureDetectors;
using RaftNET.Records;

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
        Assert.That(output.LogEntries.First().Fake, Is.Not.Null);
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
        Assert.That(output.LogEntries.First().Fake, Is.Not.Null);
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
        Assert.That(output.LogEntries.First().Fake, Is.Not.Null);
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
    public void ReplaceTwoNodesSimple() {
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
    public void ReplaceTwoNodes() {
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
    public void RemoveAndAddNodes() {
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
    public void LargeClusterAddTwoNodes() {
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
        a.AddEntry(new Void());
        a.AddEntry(Messages.ConfigFromIds(A_ID, B_ID, C_ID, D_ID, E_ID, F_ID, G_ID));
        a.AddEntry(new Void());
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
    public void TestConfigurationChangeAToB() {
        // Test we can transition from a single-server configuration
        // {A} to a single server configuration {B}
        var log = new Log(new SnapshotDescriptor {
            Idx = 0,
            Config = Messages.ConfigFromIds(A_ID)
        });
        var a = CreateFollower(A_ID, log);
        ElectionTimeout(a);
        Assert.That(a.IsLeader, Is.True);
        // Let's have a non-empty log at A
        a.AddEntry(new Void());

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
