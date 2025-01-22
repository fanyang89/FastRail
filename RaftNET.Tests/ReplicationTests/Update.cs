using OneOf;

namespace RaftNET.Tests.ReplicationTests;

public class Update : OneOfBase<Entries, NewLeader, Reset, WaitLog, SetConfig, Tick, ReadValue, UpdateRpc, UpdateFault> {
    public Update(Entries input) : base(input) {}
}
