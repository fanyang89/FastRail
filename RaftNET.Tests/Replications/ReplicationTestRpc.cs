using RaftNET.Services;

namespace RaftNET.Tests.Replications;

public class ReplicationTestRpc : IRaftRpcClient {
    public int Id;
    public Connected Connected;
    public Snapshots Snapshots;
    public RpcNet Net;
    public RpcConfig RpcConfig;
    public ServerAddressSet KnownPeers { get; }
    public uint ServersAdded = 0;
    public uint ServersRemoved = 0;
    public int SameNodePrefix;
    public bool Delays;

    public bool DropPackets() {
        return RpcConfig.Drops && Random.Shared.Next() % 5 == 0;
    }

    public bool IsLocalNode(int id) {
        return (id & SameNodePrefix) == (Id & SameNodePrefix);
    }

    public TimeSpan GetDelay(int id) {
        return IsLocalNode(id) ? RpcConfig.LocalDelay : RpcConfig.NetworkDelay;
    }

    public TimeSpan RandExtraDelay() {
        return TimeSpan.FromMilliseconds(Random.Shared.NextInt64(0, RpcConfig.ExtraDelayMax.Milliseconds));
    }

    public Task Ping(DateTime deadline) {
        throw new NotImplementedException();
    }

    public Task VoteRequest(VoteRequest request) {
        throw new NotImplementedException();
    }

    public Task VoteResponse(VoteResponse response) {
        throw new NotImplementedException();
    }

    public Task AppendRequest(AppendRequest request) {
        throw new NotImplementedException();
    }

    public Task AppendResponse(AppendResponse request) {
        throw new NotImplementedException();
    }

    public Task<SnapshotResponse> SendSnapshot(InstallSnapshotRequest request) {

    }

    public Task TimeoutNowRequest(TimeoutNowRequest request) {
        throw new NotImplementedException();
    }
}
