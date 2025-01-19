using RaftNET.Services;
using RaftNET.Tests.Exceptions;

namespace RaftNET.Tests.ReplicationTests;

public sealed class MockRpc : RaftService {
    public ulong Id;
    public Connected Connected;
    public Snapshots Snapshots;
    public RpcNet Net;
    public RpcConfig RpcConfig;
    public ServerAddressSet KnownPeers { get; }
    public uint ServersAdded = 0;
    public uint ServersRemoved = 0;
    public ulong SameNodePrefix;
    public bool Delays;
    private ulong? _delaySnapshotId;
    private SemaphoreSlim _delaySnapshot = new(1, 1);

    public MockRpc(RaftServiceConfig config, ulong id, Connected connected, Snapshots snapshots, RpcNet net,
        RpcConfig rpcConfig, uint serversAdded, uint serversRemoved, ulong sameNodePrefix, bool delays,
        ServerAddressSet knownPeers) : base(config) {
        Id = id;
        Connected = connected;
        Snapshots = snapshots;
        Net = net;
        RpcConfig = rpcConfig;
        ServersAdded = serversAdded;
        ServersRemoved = serversRemoved;
        SameNodePrefix = sameNodePrefix;
        Delays = delays;
        KnownPeers = knownPeers;
    }

    public bool DropPackets() {
        return RpcConfig.Drops && Random.Shared.Next() % 5 == 0;
    }

    public bool IsLocalNode(ulong id) {
        return (id & SameNodePrefix) == (Id & SameNodePrefix);
    }

    public TimeSpan GetDelay(ulong id) {
        return IsLocalNode(id) ? RpcConfig.LocalDelay : RpcConfig.NetworkDelay;
    }

    public TimeSpan RandExtraDelay() {
        return TimeSpan.FromMilliseconds(Random.Shared.NextInt64(0, RpcConfig.ExtraDelayMax.Milliseconds));
    }

    public void DelaySendSnapshot(ulong snapshotId) {
        _delaySnapshotId = snapshotId;
    }

    public async Task<SnapshotResponse> SendSnapshot(ulong to, InstallSnapshotRequest request) {
        if (!Net.ContainsKey(to)) {
            throw new UnknownNodeException(to);
        }
        if (Connected.IsConnected(Id, to)) {
            throw new DisconnectedException(Id, to);
        }

        // transfer the snapshot
        var snapshotId = request.Snp.Id;
        Snapshots[to][snapshotId] = Snapshots[Id][snapshotId];
        if (_delaySnapshotId != null) {
            await _delaySnapshot.WaitAsync();
            _delaySnapshot.Release(1);
        }
        return await Net[to].HandleInstallSnapshotRequestAsync(Id, request);
    }
}
