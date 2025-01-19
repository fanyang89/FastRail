using RaftNET.Tests.ReplicationTests;

namespace RaftNET.Tests;

public class ReplicationTest : ReplicationTestBase {
    [TestCase(false, false)]
    [TestCase(false, true)]
    [TestCase(true, false)]
    [TestCase(true, true)]
    public async Task TestSimpleReplicationAsync(bool preVote, bool drops) {
        // 1 node, simple replication, empty, no updates
        var test = new ReplicationTestCase { Nodes = 1 };
        var rpcConfig = new RpcConfig { Drops = drops };
        await RunReplicationTest(test, preVote, DefaultTickDelta, rpcConfig);
    }

    [TestCase(false, false)]
    [TestCase(false, true)]
    [TestCase(true, false)]
    [TestCase(true, true)]
    public async Task TestNonEmptyLeaderLogAsync(bool preVote, bool drops) {
        // 2 nodes, 4 existing leader entries, 4 updates
        var test = new ReplicationTestCase {
            Nodes = 2,
            InitialStates = new List<List<LogEntrySlim>> {
                new() {
                    new LogEntrySlim(1, 0),
                    new LogEntrySlim(1, 1),
                    new LogEntrySlim(1, 2),
                    new LogEntrySlim(1, 3),
                }
            },
            Updates = [new Update(new Entries(4))]
        };
        var rpcConfig = new RpcConfig { Drops = drops };
        await RunReplicationTest(test, preVote, DefaultTickDelta, rpcConfig);
    }
}
