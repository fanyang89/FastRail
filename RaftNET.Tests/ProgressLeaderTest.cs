using System.Text;
using RaftNET.FailureDetectors;
using RaftNET.Replication;

namespace RaftNET.Tests;

public class ProgressLeaderTest : FSMTestBase {
    [Test]
    public void TestProgressLeader() {
        var cfg = Messages.ConfigFromIds(Id1, Id2);
        var log = new Log(new SnapshotDescriptor { Config = cfg });
        var fsm = new FSMDebug(Id1, 0, 0, log, new TrivialFailureDetector(), FSMConfig);

        ElectionTimeout(fsm);
        Assert.That(fsm.IsCandidate, Is.True);

        var output = fsm.GetOutput();
        Assert.That(output.TermAndVote, Is.Not.Null);
        fsm.Step(Id2, new VoteResponse {
            CurrentTerm = output.TermAndVote.Term,
            VoteGranted = true,
            IsPreVote = false
        });
        Assert.That(fsm.IsLeader, Is.True);

        // Dummy entry local
        output = fsm.GetOutput();
        Assert.Multiple(() => {
            Assert.That(output.LogEntries, Has.Count.EqualTo(1));
            Assert.That(output.Messages, Has.Count.EqualTo(1));
        });
        Assert.That(output.LogEntries.First().DataCase, Is.EqualTo(LogEntry.DataOneofCase.Dummy));

        // accept fake entry
        var msg = output.Messages.Last().Message.AppendRequest;
        var idx = msg.Entries.Last().Idx;
        fsm.Step(Id2, new AppendResponse {
            CurrentTerm = msg.CurrentTerm,
            CommitIdx = idx,
            Accepted = new AppendAccepted {
                LastNewIdx = idx
            }
        });

        var progress = fsm.GetProgress(Id1);
        Assert.That(progress, Is.Not.Null);

        for (var i = 1; i < 6; ++i) {
            Assert.Multiple(() => {
                // NOTE: in etcd leader's own progress seems to be PIPELINE
                Assert.That(progress.State, Is.EqualTo(FollowerProgressState.Probe));
                Assert.That(progress.MatchIdx, Is.EqualTo(i));
                Assert.That(progress.NextIdx, Is.EqualTo(i + 1));
            });

            fsm.AddEntry(Encoding.UTF8.GetBytes(i.ToString()));
            output = fsm.GetOutput();

            msg = output.Messages.Last().Message.AppendRequest;
            idx = msg.Entries.Last().Idx;
            Assert.That(idx, Is.EqualTo(i + 1));
            fsm.Step(Id2, new AppendResponse {
                CurrentTerm = msg.CurrentTerm,
                CommitIdx = idx,
                Accepted = new AppendAccepted {
                    LastNewIdx = idx
                }
            });
        }
    }
}
