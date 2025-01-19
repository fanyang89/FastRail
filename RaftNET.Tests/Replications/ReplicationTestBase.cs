using Microsoft.Extensions.Logging;

namespace RaftNET.Tests.Replications;

public class ReplicationTestBase : RaftTestBase {
    protected TimeSpan DefaultTickDelta = TimeSpan.FromMilliseconds(10);
    protected ILogger<ReplicationTestBase> Logger;

    [SetUp]
    public new void Setup() {
        Logger = LoggerFactory.CreateLogger<ReplicationTestBase>();
    }

    protected async Task RunReplicationTest(ReplicationTestCase test, bool preVote, TimeSpan tickDelta,
        RpcConfig rpcConfig) {
        Logger.LogInformation("Starting test with {}",
            rpcConfig.NetworkDelay > TimeSpan.Zero ? "delays" : "no delays");

        var raftCluster = new RaftCluster(test, ApplyChanges, test.TotalValues, test.GetFirstValue(), test.InitialLeader,
            preVote, tickDelta, rpcConfig);
        await raftCluster.StartAll();

        Logger.LogInformation("Processing updates");

        foreach (var testUpdate in test.Updates) {
            await testUpdate.Match<Task>(
                entries => entries.Concurrent
                    ? raftCluster.AddEntriesConcurrent(entries.N, entries.Server)
                    : raftCluster.AddEntries(entries.N, entries.Server),
                newLeader => raftCluster.ElectNewLeader(newLeader.Id),
                reset => raftCluster.Reset(reset),
                waitLog => raftCluster.WaitLog(waitLog),
                setConfig => raftCluster.ChangeConfiguration(setConfig),
                tick => raftCluster.Tick(tick),
                readValue => raftCluster.Read(readValue),
                updateRpc => updateRpc.Match<Task>(
                    checkRpcConfig => raftCluster.CheckRpcConfig(checkRpcConfig),
                    checkRpcAdded => raftCluster.CheckRpcAdded(checkRpcAdded),
                    checkRpcRemoved => raftCluster.CheckRpcRemoved(checkRpcRemoved),
                    rpcResetCounters => raftCluster.RpcResetCounters(rpcResetCounters)),
                updateFault => updateFault.Match<Task>(
                    partition => raftCluster.Partition(partition),
                    isolate => raftCluster.Isolate(isolate),
                    disconnect => raftCluster.Disconnect(disconnect),
                    stop => raftCluster.Stop(stop))
            );
        }

        raftCluster.ConnectAll();
        raftCluster.ReconfigureAll();

        if (test.TotalValues > 0) {
            Logger.LogInformation("Appending remaining values");
            raftCluster.AddRemainingEntries();
            raftCluster.WaitAll();
        }

        raftCluster.StopAll();

        if (test.TotalValues > 0) {
            raftCluster.Verify();
        }
    }

    protected int ApplyChanges(ulong id, List<Command> commands, HasherInt hasher) {
        Logger.LogInformation("[{}] ApplyChanges() got entries, count={}", id, commands.Count);
        var entries = 0;
        foreach (var command in commands) {
            var n = BitConverter.ToInt32(command.Buffer.Span);
            if (n != int.MinValue) {
                entries++;
                hasher.Update(n);
                Logger.LogInformation("[{}] Apply changes, n={}", id, n);
            }
        }
        return entries;
    }
}
