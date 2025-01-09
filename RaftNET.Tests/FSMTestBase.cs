using Microsoft.Extensions.Logging;
using RaftNET.FailureDetectors;
using RaftNET.Records;

namespace RaftNET.Tests;

public class FSMTestBase : RaftTestBase {
    protected const ulong Id1 = 1;
    protected const ulong Id2 = 2;
    protected const ulong Id3 = 3;
    protected const ulong Id4 = 4;
    protected const ulong Id5 = 5;

    protected const ulong A_ID = 1;
    protected const ulong B_ID = 2;
    protected const ulong C_ID = 3;
    protected const ulong D_ID = 4;
    protected const ulong E_ID = 5;

    protected readonly FSM.Config FSMConfig = new(
        MaxLogSize: 4 * 1024 * 1024,
        AppendRequestThreshold: 1,
        EnablePreVote: false
    );

    protected readonly FSM.Config FSMPreVoteConfig = new(
        MaxLogSize: 4 * 1024 * 1024,
        AppendRequestThreshold: 1,
        EnablePreVote: true
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

    protected FSMDebug CreateFollower(ulong id, Log log, IFailureDetector fd) {
        return new FSMDebug(id, 0, 0, log, fd, FSMConfig, LoggerFactory.CreateLogger<FSM>());
    }

    protected FSMDebug CreateFollower(ulong id, Log log) {
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

    private static bool Deliver(Dictionary<ulong, FSM> routes, ulong from, ToMessage m) {
        if (!routes.TryGetValue(m.To, out var to)) {
            return false;
        }
        to.Step(from, m.Message);
        return true;
    }
}