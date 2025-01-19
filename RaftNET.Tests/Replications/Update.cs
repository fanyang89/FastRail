using OneOf;

namespace RaftNET.Tests.Replications;

public class Update : OneOfBase<Entries, NewLeader, Reset, WaitLog, SetConfig, Tick, ReadValue, UpdateRpc, UpdateFault> {
    protected Update(OneOf<Entries, NewLeader, Reset, WaitLog, SetConfig, Tick, ReadValue, UpdateRpc, UpdateFault> input) :
        base(input) {}

    public Update(Entries input) : base(input) {}
}