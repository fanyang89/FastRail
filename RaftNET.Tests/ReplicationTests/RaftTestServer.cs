using RaftNET.Services;

namespace RaftNET.Tests.ReplicationTests;

public class RaftTestServer {
    public RaftService Service { get; set; }
    public MockRpc Rpc { get; set; }
    public TestStateMachine StateMachine { get; set; }

    public async Task StartAsync() {
        await Task.CompletedTask;
    }
}
