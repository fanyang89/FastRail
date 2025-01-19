using RaftNET.Services;

namespace RaftNET.Tests.Replications;

public class RaftTestServer {
    public RaftService Service { get; set; }
    public ReplicationTestRpc Rpc { get; set; }
    public TestStateMachine StateMachine { get; set; }

    public async Task Start() {
        await Task.CompletedTask;
    }
}
