using OneOf;

namespace RaftNET.Tests.Replications;

public class UpdateRpc : OneOfBase<CheckRpcConfig, CheckRpcAdded, CheckRpcRemoved, RpcResetCounters> {
    protected UpdateRpc(OneOf<CheckRpcConfig, CheckRpcAdded, CheckRpcRemoved, RpcResetCounters> input) : base(input) {}
}