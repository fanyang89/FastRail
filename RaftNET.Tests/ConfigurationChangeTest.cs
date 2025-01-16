using RaftNET.Exceptions;
using RaftNET.FailureDetectors;

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
}
