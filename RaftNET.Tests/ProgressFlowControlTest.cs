using Google.Protobuf;
using RaftNET.FailureDetectors;
using RaftNET.Replication;

namespace RaftNET.Tests;

public class ProgressFlowControlTest : FSMTestBase {
    [Test]
    public void TestProgressFlowControl() {
        var cfg = Messages.ConfigFromIds(Id1, Id2);
        var log = new Log(new SnapshotDescriptor { Config = cfg });

        // Fit 2 x 1000 sized blobs
        var fsmCfg8 = FSMConfig.Clone();
        fsmCfg8.AppendRequestThreshold = 2000;
        var fsm = new FSMDebug(Id1, 0, 0, log, new TrivialFailureDetector(), fsmCfg8);

        ElectionTimeout(fsm);
        var output = fsm.GetOutput();
        Assert.That(output.TermAndVote, Is.Not.Null);
        var currentTerm = output.TermAndVote.Term;
        fsm.Step(Id2, new VoteResponse { CurrentTerm = currentTerm, VoteGranted = true });
        Assert.That(fsm.IsLeader, Is.True);

        fsm.Step(Id2, new VoteResponse { CurrentTerm = currentTerm, VoteGranted = true });
        var fprogress = fsm.GetProgress(Id2);
        Assert.That(fprogress, Is.Not.Null);
        Assert.That(fprogress.State, Is.EqualTo(FollowerProgressState.Probe));

        // While node 2 is in probe state, propose a bunch of entries.
        var blob = new string('a', 1000);
        var blobSize = new Command { Buffer = ByteString.CopyFromUtf8(blob) }.CalculateSize();
        for (var i = 0; i < 10; i++) {
            fsm.AddEntry(blob);
        }
        output = fsm.GetOutput();

        Assert.That(output.Messages.Count, Is.EqualTo(1));

        Assert.That(output.Messages.Last().Message.IsAppendRequest, Is.True);
        var msg = output.Messages.Last().Message.AppendRequest;

        // The first proposal (only one proposal gets sent because follower is in probe state)
        // Also, the first append request by new leader is dummy
        Assert.That(msg.Entries.Count, Is.EqualTo(1));
        var le = msg.Entries.Last();
        ulong currentEntry = 0;
        Assert.Multiple(() => {
            Assert.That(le.Idx, Is.EqualTo(++currentEntry));
            Assert.That(le.DataCase, Is.EqualTo(LogEntry.DataOneofCase.Fake));
        });

        // When this appending is acked, we change to replicate state and can
        // send multiple messages at once. (PIPELINE)
        fsm.Step(Id2,
            new AppendResponse
                { CurrentTerm = msg.CurrentTerm, CommitIdx = le.Idx, Accepted = new AppendAccepted { LastNewIdx = le.Idx } });
        var fprogress2 = fsm.GetProgress(Id2);
        Assert.That(fprogress2, Is.Not.Null);
        Assert.That(fprogress2.State, Is.EqualTo(FollowerProgressState.Pipeline));

        output = fsm.GetOutput();
        // 10 entries: first in 1 msg, then 10 remaining 2 per msg = 5
        Assert.That(output.Messages, Has.Count.EqualTo(5));

        ulong committed = 1;
        Assert.That(output.Committed, Has.Count.EqualTo(1));

        for (int i = 0; i < output.Messages.Count; i++) {
            Assert.That(output.Messages[i].Message.IsAppendRequest, Is.True);
            var message = output.Messages[i].Message.AppendRequest;
            Assert.That(message.Entries, Has.Count.EqualTo(2));
            foreach (var logEntry in message.Entries) {
                Assert.Multiple(() => {
                    Assert.That(logEntry.Idx, Is.EqualTo(++currentEntry));
                    Assert.That(logEntry.DataCase, Is.EqualTo(LogEntry.DataOneofCase.Command));
                });
                var cmd = logEntry.Command;
                Assert.That(cmd.CalculateSize(), Is.EqualTo(blobSize));
            }
        }

        var ackIdx = currentEntry;
        fsm.Step(Id2, new AppendResponse {
            CurrentTerm = msg.CurrentTerm,
            CommitIdx = ackIdx,
            Accepted = new AppendAccepted {
                LastNewIdx = ackIdx
            }
        });
        output = fsm.GetOutput();
        committed = currentEntry - committed;
        Assert.That(output.Committed.Count, Is.EqualTo(committed));

    }
}
