using Microsoft.Extensions.Logging;

namespace RaftNET.Tests.ReplicationTests;

public class ReplicationTestBase : RaftTestBase {
    protected TimeSpan DefaultTickDelta = TimeSpan.FromMilliseconds(10);
    protected ILogger<ReplicationTestBase> Logger;

    [SetUp]
    public new void Setup() {
        Logger = LoggerFactory.CreateLogger<ReplicationTestBase>();
    }

    protected async Task RunReplicationTestAsync(ReplicationTestCase test, bool preVote, TimeSpan tickDelta,
        RpcConfig rpcConfig) {
        Logger.LogInformation("Starting test with {}",
            rpcConfig.NetworkDelay > TimeSpan.Zero ? "delays" : "no delays");

        var raftCluster = new RaftCluster(test, ApplyChanges, test.TotalValues, test.GetFirstValue(), test.InitialLeader,
            preVote, tickDelta, rpcConfig);
        await raftCluster.StartAllAsync();

        Logger.LogInformation("Processing updates");

        foreach (var testUpdate in test.Updates) {
            await testUpdate.Match<Task>(
                entries => entries.Concurrent
                    ? raftCluster.AddEntriesConcurrentAsync(entries.N, entries.Server)
                    : raftCluster.AddEntriesAsync(entries.N, entries.Server),
                newLeader => raftCluster.ElectNewLeaderAsync(newLeader.Id),
                reset => raftCluster.ResetAsync(reset),
                waitLog => raftCluster.WaitLogAsync(waitLog),
                setConfig => raftCluster.ChangeConfigurationAsync(setConfig),
                tick => raftCluster.TickAsync(tick),
                readValue => raftCluster.ReadAsync(readValue),
                updateRpc => updateRpc.Match<Task>(
                    checkRpcConfig => raftCluster.CheckRpcConfigAsync(checkRpcConfig),
                    checkRpcAdded => raftCluster.CheckRpcAddedAsync(checkRpcAdded),
                    checkRpcRemoved => raftCluster.CheckRpcRemovedAsync(checkRpcRemoved),
                    rpcResetCounters => raftCluster.RpcResetCountersAsync(rpcResetCounters)),
                updateFault => updateFault.Match<Task>(
                    partition => raftCluster.PartitionAsync(partition),
                    isolate => raftCluster.IsolateAsync(isolate),
                    disconnect => raftCluster.DisconnectAsync(disconnect),
                    stop => raftCluster.StopAsync(stop))
            );
        }

        raftCluster.ConnectAll();
        raftCluster.ReconfigureAllAsync();

        if (test.TotalValues > 0) {
            Logger.LogInformation("Appending remaining values");
            raftCluster.AddRemainingEntriesAsync();
            raftCluster.WaitAllAsync();
        }

        raftCluster.StopAllAsync();

        if (test.TotalValues > 0) {
            raftCluster.Verify();
        }
    }

    protected int ApplyChanges(ulong id, List<Command> commands, HasherInt hasher) {
        Logger.LogInformation("[{}] ApplyChanges() got entries, count={}", id, commands.Count);
        var entries = 0;
        foreach (var command in commands) {
            var n = BitConverter.ToUInt64(command.Buffer.Span);
            if (n != ulong.MinValue) {
                entries++;
                hasher.Update(n);
                Logger.LogInformation("[{}] Apply changes, n={}", id, n);
            }
        }
        return entries;
    }
}
