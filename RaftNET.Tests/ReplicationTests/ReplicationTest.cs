namespace RaftNET.Tests.ReplicationTests;

public class ReplicationTest : ReplicationTestBase {
    [TestCase(false, false)]
    [TestCase(false, true)]
    [TestCase(true, false)]
    [TestCase(true, true)]
    public async Task TestSimpleReplicationAsync(bool preVote, bool drops) {
        // 1 node, simple replication, empty, no updates
        var test = new ReplicationTestCase { Nodes = 1 };
        var rpcConfig = new RpcConfig { Drops = drops };
        await RunReplicationTestAsync(test, preVote, DefaultTickDelta, rpcConfig);
    }
}
