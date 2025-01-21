using RaftNET.FailureDetectors;

namespace RaftNET.Tests;

public class LogReplicationTest : FSMTestBase {
    [Test]
    public void TestLogReplication1() {
        var cfg = Messages.ConfigFromIds(Id1, Id2, Id3);
        var log = new RaftLog(new SnapshotDescriptor { Config = cfg });
        var fsm = new FSMDebug(Id1, 0, 0, log, new TrivialFailureDetector(), FSMConfig);

        ElectionTimeout(fsm);
        Assert.That(fsm.IsCandidate, Is.True);
        var output = fsm.GetOutput();
        Assert.That(output.TermAndVote, Is.Not.Null);
        var currentTerm = output.TermAndVote.Term;
        Assert.That(currentTerm, Is.Not.Zero);
        fsm.Step(Id2, new VoteResponse { CurrentTerm = currentTerm, VoteGranted = true });
        Assert.That(fsm.IsLeader, Is.True);
        output = fsm.GetOutput();
        Assert.That(output.LogEntries, Has.Count.EqualTo(1));
        Assert.Multiple(() => {
            Assert.That(output.LogEntries.First().DataCase, Is.EqualTo(LogEntry.DataOneofCase.Fake));
            Assert.That(output.Committed, Is.Empty);
            Assert.That(output.Messages, Has.Count.EqualTo(2));
        });
        const ulong fakeIdx = 1;

        foreach (var message in output.Messages) {
            Assert.That(message.Message.IsAppendRequest, Is.True);
            var msg = message.Message.AppendRequest;
            Assert.Multiple(() => {
                Assert.That(msg.PrevLogIdx, Is.Zero);
                Assert.That(msg.PrevLogTerm, Is.Zero);
                Assert.That(msg.Entries, Has.Count.EqualTo(1));
            });
            var lep = msg.Entries.Last();
            Assert.Multiple(() => {
                Assert.That(lep.Idx, Is.EqualTo(fakeIdx));
                Assert.That(lep.Term, Is.EqualTo(currentTerm));
                Assert.That(lep.DataCase, Is.EqualTo(LogEntry.DataOneofCase.Fake));
            });
            fsm.Step(message.To, new AppendResponse {
                CurrentTerm = msg.CurrentTerm,
                CommitIdx = fakeIdx,
                Accepted = new AppendAccepted {
                    LastNewIdx = fakeIdx
                }
            });
        }

        output = fsm.GetOutput();
        Assert.That(output.Committed, Has.Count.EqualTo(1));

        // Add data entry
        fsm.AddEntry("1");
        output = fsm.GetOutput();
        Assert.Multiple(() => {
            Assert.That(output.LogEntries, Has.Count.EqualTo(1));
            Assert.That(output.Messages, Has.Count.EqualTo(2));
        });
        const int entryIdx = 2;

        foreach (var message in output.Messages) {
            Assert.That(message.Message.IsAppendRequest, Is.True);
            var msg = message.Message.AppendRequest;
            Assert.Multiple(() => {
                Assert.That(msg.PrevLogIdx, Is.EqualTo(1));
                Assert.That(msg.PrevLogTerm, Is.EqualTo(currentTerm));
                Assert.That(msg.Entries, Has.Count.EqualTo(1));
            });
            var lep = msg.Entries.Last();
            Assert.Multiple(() => {
                Assert.That(lep.Idx, Is.EqualTo(entryIdx));
                Assert.That(lep.Term, Is.EqualTo(currentTerm));
                Assert.That(lep.DataCase, Is.EqualTo(LogEntry.DataOneofCase.Command));
            });
            var idx = lep.Idx;
            fsm.Step(message.To, new AppendResponse {
                CurrentTerm = msg.CurrentTerm,
                CommitIdx = lep.Idx,
                Accepted = new AppendAccepted {
                    LastNewIdx = idx
                }
            });
        }

        output = fsm.GetOutput();
        Assert.That(output.Committed, Has.Count.EqualTo(1));
    }

    [Test]
    public void TestLogReplication2() {
        var cfg = Messages.ConfigFromIds(Id1, Id2, Id3);
        var log = new RaftLog(new SnapshotDescriptor { Config = cfg });
        var fsm = new FSMDebug(Id1, 0, 0, log, new TrivialFailureDetector(), FSMConfig);

        ElectionTimeout(fsm);
        var output = fsm.GetOutput();
        Assert.That(output.TermAndVote, Is.Not.Null);
        var currentTerm = output.TermAndVote.Term;
        fsm.Step(Id2, new VoteResponse { CurrentTerm = currentTerm, VoteGranted = true });
        Assert.That(fsm.IsLeader, Is.True);
        output = fsm.GetOutput();
        Assert.That(output.Messages, Has.Count.EqualTo(2));

        const ulong fakeIdx = 1; // Nothing before it
        foreach (var message in output.Messages) {
            Assert.That(message.Message.IsAppendRequest, Is.True);
            var msg = message.Message.AppendRequest;
            fsm.Step(message.To, new AppendResponse {
                CurrentTerm = msg.CurrentTerm,
                CommitIdx = fakeIdx,
                Accepted = new AppendAccepted { LastNewIdx = fakeIdx }
            });
        }

        output = fsm.GetOutput();
        Assert.That(output.Committed, Has.Count.EqualTo(1));

        // Add 1st data entry
        fsm.AddEntry("1");
        output = fsm.GetOutput();
        Assert.Multiple(() => {
            Assert.That(output.LogEntries, Has.Count.EqualTo(1));
            Assert.That(output.Messages, Has.Count.EqualTo(2));
        });

        // ACK 1st entry
        fsm.Step(Id2,
            new AppendResponse { CurrentTerm = currentTerm, CommitIdx = 2, Accepted = new AppendAccepted { LastNewIdx = 2 } });
        output = fsm.GetOutput();
        Assert.That(output.Committed, Has.Count.EqualTo(1));

        // Add 2nd data entry
        fsm.AddEntry("2");
        output = fsm.GetOutput();
        Assert.Multiple(() => {
            Assert.That(output.LogEntries, Has.Count.EqualTo(1));
            Assert.That(output.Messages, Has.Count.EqualTo(2));
        });

        // ACK 2nd entry
        const ulong secondIdx = 3;
        foreach (var message in output.Messages) {
            Assert.That(message.Message.IsAppendRequest, Is.True);
            var msg = message.Message.AppendRequest;
            Assert.Multiple(() => {
                Assert.That(msg.PrevLogIdx, Is.EqualTo(2));
                Assert.That(msg.PrevLogTerm, Is.EqualTo(currentTerm));
                Assert.That(msg.Entries, Has.Count.EqualTo(1));
            });
            var lep = msg.Entries.Last();
            Assert.Multiple(() => {
                Assert.That(lep.Idx, Is.EqualTo(secondIdx));
                Assert.That(lep.Term, Is.EqualTo(currentTerm));
            });
            fsm.Step(message.To, new AppendResponse {
                CurrentTerm = msg.CurrentTerm,
                CommitIdx = secondIdx,
                Accepted = new AppendAccepted { LastNewIdx = secondIdx }
            });
        }

        output = fsm.GetOutput();
        Assert.That(output.Committed, Has.Count.EqualTo(1));
    }
}
