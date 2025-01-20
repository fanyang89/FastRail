using RaftNET.Services;

namespace RaftNET.Tests.ReplicationTests;

public class RaftTestServer(RaftService raft, TestStateMachine sm, MockRpc rpc) {
    public RaftService Service { get; } = raft;
    public MockRpc Rpc { get; } = rpc;
    public TestStateMachine StateMachine { get; } = sm;

    public async Task StartAsync() {
        throw new NotImplementedException();
    }
}
