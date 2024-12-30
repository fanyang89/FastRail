using RaftNET.Exceptions;

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
        fsm.Step(Id2, new AppendResponse {
            CurrentTerm = msg.CurrentTerm, CommitIdx = idx,
            Accepted = new AppendAccepted { LastNewIdx = idx }
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

        fsm.Step(Id2, new AppendResponse {
            CurrentTerm = msg.CurrentTerm, CommitIdx = idx,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });
        Assert.That(fsm.GetConfiguration().IsJoint, Is.False);
        Assert.Throws<ConfigurationChangeInProgressException>(() => fsm.AddEntry(newConfig));
        output = fsm.GetOutput();
        Assert.That(output.LogEntries.Count, Is.EqualTo(1));
        Assert.That(output.Messages.Count, Is.GreaterThanOrEqualTo(1));

        msg = output.Messages.Last().Message.AppendRequest;
        idx = msg.Entries.Last().Idx;
        fsm.Step(Id2, new AppendResponse {
            CurrentTerm = msg.CurrentTerm, CommitIdx = idx,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });
        Assert.That(fsm.GetConfiguration().Current.Count, Is.EqualTo(3));
        fsm.Step(Id3, new AppendResponse {
            CurrentTerm = msg.CurrentTerm, CommitIdx = idx,
            Accepted = new AppendAccepted { LastNewIdx = idx }
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
        fsm.Step(Id2, new AppendResponse {
            CurrentTerm = msg.CurrentTerm, CommitIdx = idx,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });
        fsm.Step(Id3, new AppendResponse {
            CurrentTerm = msg.CurrentTerm, CommitIdx = idx,
            Accepted = new AppendAccepted { LastNewIdx = idx }
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
        fsm.Step(Id2, new AppendResponse {
            CurrentTerm = msg.CurrentTerm, CommitIdx = idx,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });
        output = fsm.GetOutput();
        Assert.That(output.Messages.Count, Is.EqualTo(1));
        msg = output.Messages.First().Message.AppendRequest;
        Assert.That(output.LogEntries.First().DataCase, Is.EqualTo(LogEntry.DataOneofCase.Configuration));
        idx = msg.Entries.Last().Idx;
        Assert.That(idx, Is.EqualTo(103));
        Assert.That(fsm.GetConfiguration().Current.Count, Is.EqualTo(2));
        Assert.That(fsm.GetConfiguration().IsJoint, Is.False);
        fsm.Step(Id2, new AppendResponse {
            CurrentTerm = msg.CurrentTerm, CommitIdx = idx,
            Accepted = new AppendAccepted { LastNewIdx = idx }
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
        fsm.Step(Id2, new VoteResponse {
            CurrentTerm = output.TermAndVote.Term, VoteGranted = true
        });
        Assert.That(fsm.IsLeader, Is.True);
        output = fsm.GetOutput();
        Assert.That(output.LogEntries.Count, Is.EqualTo(1));
        Assert.That(output.LogEntries.First().Dummy, Is.Not.Null);
        Assert.That(output.Committed.Count, Is.EqualTo(0));
        Assert.That(output.Messages.Count, Is.EqualTo(2));
        var msg = output.Messages.Last().Message.AppendRequest;
        var idx = msg.Entries.Last().Idx;
        fsm.Step(Id2, new AppendResponse {
            CurrentTerm = msg.CurrentTerm, CommitIdx = idx,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });
        fsm.Step(Id3, new AppendResponse {
            CurrentTerm = msg.CurrentTerm, CommitIdx = idx,
            Accepted = new AppendAccepted { LastNewIdx = idx }
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
        fsm.Step(Id2, new AppendResponse {
            CurrentTerm = msg.CurrentTerm, CommitIdx = idx,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });
        Assert.That(fsm.GetConfiguration().IsJoint, Is.False);
        output = fsm.GetOutput();
        Assert.That(output.LogEntries.Count, Is.EqualTo(1));
        Assert.That(output.LogEntries.First().DataCase, Is.EqualTo(LogEntry.DataOneofCase.Configuration));
        Assert.That(output.Messages.Count, Is.GreaterThanOrEqualTo(1));
        msg = output.Messages.Last().Message.AppendRequest;
        idx = msg.Entries.Last().Idx;
        fsm.Step(Id2, new AppendResponse {
            CurrentTerm = msg.CurrentTerm, CommitIdx = idx,
            Accepted = new AppendAccepted { LastNewIdx = idx }
        });
        Assert.That(fsm.GetConfiguration().Current.Count, Is.EqualTo(3));
        Assert.That(fsm.GetConfiguration().IsJoint, Is.False);
    }
}