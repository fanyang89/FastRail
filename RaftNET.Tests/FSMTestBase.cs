using System.Diagnostics;
using RaftNET.FailureDetectors;
using RaftNET.Records;

namespace RaftNET.Tests;

public class FSMTestBase : RaftTestBase {
    protected const ulong A_ID = 1;
    protected const ulong B_ID = 2;
    protected const ulong C_ID = 3;
    protected const ulong D_ID = 4;
    protected const ulong E_ID = 5;
    protected const ulong F_ID = 6;
    protected const ulong G_ID = 7;
    protected const ulong H_ID = 8;
    protected const ulong Id1 = 1;
    protected const ulong Id2 = 2;
    protected const ulong Id3 = 3;
    protected const ulong Id4 = 4;
    protected const ulong Id5 = 5;

    protected readonly FSM.Config FSMConfig = new(
        maxLogSize: 4 * 1024 * 1024,
        appendRequestThreshold: 1,
        enablePreVote: false
    );

    protected readonly FSM.Config FSMPreVoteConfig = new(
        maxLogSize: 4 * 1024 * 1024,
        appendRequestThreshold: 1,
        enablePreVote: true
    );

    protected void ElectionTimeout(FSM fsm) {
        for (var i = 0; i <= 2 * FSM.ElectionTimeout; ++i) fsm.Tick();
    }

    protected void ElectionThreshold(FSM fsm) {
        for (var i = 0; i < FSM.ElectionTimeout; ++i) fsm.Tick();
    }

    protected void MakeCandidate(FSM fsm) {
        Assert.That(fsm.IsFollower, Is.True);

        while (fsm.IsFollower) fsm.Tick();
    }

    protected FSMDebug CreateFollower(ulong id, RaftLog log, IFailureDetector fd) {
        return new FSMDebug(id, 0, 0, log, fd, FSMConfig);
    }

    protected FSMDebug CreateFollower(ulong id, RaftLog log) {
        return CreateFollower(id, log, new TrivialFailureDetector());
    }

    protected static void Communicate(params FSM[] fsmList) {
        CommunicateUntil(() => false, fsmList);
    }

    protected static void CommunicateUntil(Func<bool> shouldStop, params FSM[] fsmList) {
        var map = fsmList.ToDictionary(fsm => fsm.Id);
        CommunicateImpl(shouldStop, map);
    }

    protected static void CommunicateImpl(Func<bool> shouldStop, Dictionary<ulong, FSM> routes) {
        bool hasTraffic;
        do {
            hasTraffic = false;
            foreach (var from in routes.Values) {
                for (var hasOutput = from.HasOutput(); hasOutput; hasOutput = from.HasOutput()) {
                    var output = from.GetOutput();
                    if (shouldStop()) {
                        return;
                    }
                    foreach (var m in output.Messages) {
                        hasTraffic = true;
                        if (Deliver(routes, from.Id, m) && shouldStop()) {
                            return;
                        }
                    }
                }
            }
        } while (hasTraffic);
    }

    protected static FSM SelectLeader(params FSM[] fsmList) {
        foreach (var fsm in fsmList) {
            if (fsm.IsLeader) {
                return fsm;
            }
        }
        throw new UnreachableException("No leader");
    }

    protected static bool Deliver(Dictionary<ulong, FSM> routes, ulong from, ToMessage m) {
        if (!routes.TryGetValue(m.To, out var to)) {
            return false;
        }
        to.Step(from, m.Message);
        return true;
    }

    protected static void Deliver(Dictionary<ulong, FSM> routes, ulong from, IList<ToMessage> messages) {
        foreach (var message in messages) {
            Deliver(routes, from, message);
        }
    }

    protected bool RollDice(float probability = 0.5f) {
        return Random.Shared.NextSingle() < probability;
    }

    protected static bool CompareLogEntries(RaftLog log1, RaftLog log2, ulong from, ulong to) {
        Debug.Assert(to <= log1.LastIdx());
        Debug.Assert(to <= log2.LastIdx());
        for (var i = from; i < to; i++) {
            if (!CompareLogEntry(log1[i], log2[i])) {
                return false;
            }
        }
        return true;
    }

    protected static bool CompareLogEntry(LogEntry le1, LogEntry le2) {
        return le1.Term == le2.Term && le1.Idx == le2.Idx && le1.DataCase == le2.DataCase;
    }
}
