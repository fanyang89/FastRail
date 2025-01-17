using RaftNET.FailureDetectors;
using RaftNET.Records;

namespace RaftNET.Tests;

public class ReorderedRejectTest : FSMTestBase {
    [Test]
    public void TestReorderedReject() {
        var fsm1 = new FSMDebug(Id1, 1, 0, new Log(new SnapshotDescriptor { Config = Messages.ConfigFromIds(Id1) }),
            new TrivialFailureDetector(), FSMConfig);

        while (!fsm1.IsLeader) {
            fsm1.Tick();
        }
        fsm1.AddEntry(new Dummy());
        fsm1.GetOutput();

        var fsm2 = new FSMDebug(Id2, 1, 0, new Log(new SnapshotDescriptor { Config = new Configuration() }),
            new TrivialFailureDetector(), FSMConfig);

        var routes = new Dictionary<ulong, FSM> {
            { fsm1.Id, fsm1 },
            { fsm2.Id, fsm2 }
        };
        fsm1.AddEntry(Messages.ConfigFromIds(fsm1.Id, fsm2.Id));
        fsm1.Tick();

        // fsm1 sends append_entries with idx=2 to fsm2
        var appendIdx21 = fsm1.GetOutput();
        fsm1.Tick();

        // fsm1 sends append_entries with idx=2 to fsm2 (again)
        var appendIdx22 = fsm1.GetOutput();
        Deliver(routes, fsm1.Id, appendIdx21.Messages);

        // fsm2 rejects the first idx=2 append
        var reject1 = fsm2.GetOutput();
        Deliver(routes, fsm1.Id, appendIdx22.Messages);

        // fsm2 rejects the second idx=2 append
        var reject2 = fsm2.GetOutput();
        Deliver(routes, fsm2.Id, reject1.Messages);

        // fsm1 sends append_entries with idx=1 to fsm2
        var appendIdx1 = fsm1.GetOutput();
        Deliver(routes, fsm1.Id, appendIdx1.Messages);

        // fsm2 accepts the idx=1 append
        var accept = fsm2.GetOutput();
        Deliver(routes, fsm2.Id, accept.Messages);
        Deliver(routes, fsm2.Id, reject2.Messages);
    }
}
