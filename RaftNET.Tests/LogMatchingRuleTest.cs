using Microsoft.Extensions.Logging;
using RaftNET.FailureDetectors;

namespace RaftNET.Tests;

public class LogMatchingRuleTest : FSMTestBase {
    private VoteResponse RequestVote(FSM fsm, ulong term, ulong lastLogIdx, ulong lastLogTerm) {
        fsm.Step(Id2, new VoteRequest { CurrentTerm = term, LastLogIdx = lastLogIdx, LastLogTerm = lastLogTerm });
        var output = fsm.GetOutput();
        return output.Messages.Last().Message.VoteResponse;
    }

    [Test]
    public void LogMatchingRule() {
        var cfg = Messages.ConfigFromIds(Id1, Id2, Id3);
        var log = new Log(new SnapshotDescriptor { Idx = 999, Config = cfg });
        log.Add(new LogEntry { Term = 10, Idx = 1000 });
        log.StableTo(log.LastIdx());

        var fsm = new FSMDebug(Id1, 10, 0, log, new TrivialFailureDetector(), FSMConfig, LoggerFactory.CreateLogger<FSM>());

        // Initial state is follower
        Assert.That(fsm.IsFollower, Is.True);

        fsm.GetOutput();

        fsm.Step(Id2, new VoteRequest { CurrentTerm = 9, LastLogIdx = 1001, LastLogTerm = 11 });
        // Current term is too old - vote is not granted
        var output = fsm.GetOutput();
        Assert.That(output.Messages.Count, Is.Zero);

        // Last stable index is too small - vote is not granted
        Assert.That(RequestVote(fsm, 11, 999, 10).VoteGranted, Is.False);
        // Last stable term is too small - vote is not granted
        Assert.That(RequestVote(fsm, 12, 1002, 9).VoteGranted, Is.False);
        // Last stable term and index are equal to the voter's - vote is granted
        Assert.That(RequestVote(fsm, 13, 1000, 10).VoteGranted, Is.True);
        // Last stable term is the same, index is greater to the voter's - vote is granted
        Assert.That(RequestVote(fsm, 14, 1001, 10).VoteGranted, Is.True);
        // Both term and index are greater than the voter's - vote is granted
        Assert.That(RequestVote(fsm, 15, 1001, 11).VoteGranted, Is.True);
    }
}