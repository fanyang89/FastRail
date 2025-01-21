using RaftNET.Services;

namespace RaftNET.Tests.ReplicationTests;

public class RaftTestServer(RaftService raft, TestStateMachine sm, MockRpc rpc) {
    public MockRpc Rpc { get; } = rpc;
    public RaftService Service { get; } = raft;
    public TestStateMachine StateMachine { get; } = sm;
}
