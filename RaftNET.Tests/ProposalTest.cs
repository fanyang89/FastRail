using RaftNET.FailureDetectors;

namespace RaftNET.Tests;

public class ProposalTest : FSMTestBase {
    [TestCase(3UL, new[] { 2UL, 3UL })]
    [TestCase(3UL, new[] { 2UL })]
    [TestCase(3UL, new ulong[] {})]
    [TestCase(4UL, new[] { 4UL })]
    [TestCase(5UL, new[] { 4UL, 5UL })]
    public void TestHandleProposal(ulong nodes, ulong[] acceptingInts) {
        var accepting = new SortedSet<ulong>();
        foreach (var a in acceptingInts) {
            accepting.Add(a);
        }

        var ids = new List<ulong>();
        for (ulong i = 1; i < nodes + 1; i++) {
            ids.Add(i);
        }

        var cfg = Messages.ConfigFromIds(ids.ToArray());
        var log1 = new Log(new SnapshotDescriptor { Config = cfg });
        var fsm1 = new FSMDebug(Id1, 0, 0, log1, new TrivialFailureDetector(), FSMConfig);

        ElectionTimeout(fsm1);
        var output1 = fsm1.GetOutput();
        Assert.Multiple(() => {
            Assert.That(output1.Messages, Has.Count.GreaterThanOrEqualTo(nodes - 1));
            Assert.That(output1.TermAndVote, Is.Not.Null);
        });
        for (ulong i = 2; i < nodes + 1; ++i) {
            fsm1.Step(i, new VoteResponse {
                CurrentTerm = output1.TermAndVote.Term,
                VoteGranted = true,
                IsPreVote = false
            });
        }
        Assert.That(fsm1.IsLeader, Is.True);
        output1 = fsm1.GetOutput();
        var lep = output1.LogEntries.Last();
        Assert.Multiple(() => {
            Assert.That(lep.DataCase, Is.EqualTo(LogEntry.DataOneofCase.Fake));
            Assert.That(output1.Messages, Has.Count.EqualTo(nodes - 1));
        });
        foreach (var (id, msg) in output1.Messages) {
            Assert.That(msg.IsAppendRequest, Is.True);
            var areq = msg.AppendRequest;
            var le = areq.Entries.Last();
            Assert.Multiple(() => {
                Assert.That(le.Idx, Is.EqualTo(1));
                Assert.That(le.DataCase, Is.EqualTo(LogEntry.DataOneofCase.Fake));
            });
            if (accepting.Contains(id)) {
                fsm1.Step(id, new AppendResponse {
                    CurrentTerm = areq.CurrentTerm,
                    CommitIdx = 1,
                    Accepted = new AppendAccepted { LastNewIdx = 1 }
                });
            }
        }
        output1 = fsm1.GetOutput();
        var commitFake = (ulong)accepting.Count + 1 >= nodes / 2 + 1;
        Assert.That(output1.Committed, Has.Count.EqualTo(commitFake ? 1 : 0));

        fsm1.AddEntry("1");
        output1 = fsm1.GetOutput();
        Assert.Multiple(() => {
            Assert.That(output1.LogEntries, Has.Count.EqualTo(1));
            Assert.That(output1.Messages.Count, Is.EqualTo(accepting.Count));
        });
        foreach (var (id, msg) in output1.Messages) {
            Assert.That(msg.IsAppendRequest, Is.True);
            var areq = msg.AppendRequest;
            var le = areq.Entries.Last();
            Assert.Multiple(() => {
                Assert.That(le.Idx, Is.EqualTo(2));
                Assert.That(le.DataCase, Is.EqualTo(LogEntry.DataOneofCase.Command));
                Assert.That(accepting, Does.Contain(id));
            });
            fsm1.Step(id, new AppendResponse {
                CurrentTerm = areq.CurrentTerm,
                CommitIdx = 2,
                Accepted = new AppendAccepted { LastNewIdx = 2 }
            });
        }
        output1 = fsm1.GetOutput();
        commitFake = (ulong)accepting.Count + 1 >= nodes / 2 + 1;
        Assert.That(output1.Committed, Has.Count.EqualTo(commitFake ? 1 : 0));
    }
}
