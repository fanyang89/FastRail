using RaftNET.Services;
using RaftNET.Tests.Exceptions;

namespace RaftNET.Tests.ReplicationTests;

public sealed class MockRpc : IRaftRpcHandler, IRaftRpcClient {
    public bool Delays;
    public uint ServersAdded = 0;
    public uint ServersRemoved = 0;
    private readonly Connected _connected;
    private readonly bool _delays;
    private readonly SemaphoreSlim _delaySnapshot = new(1, 1);
    private readonly ulong _id;
    private readonly RpcNet _net;
    private readonly RpcConfig _rpcConfig;
    private readonly Snapshots _snapshots;
    private ulong? _delaySnapshotId;
    private ulong _sameNodePrefix;

    public MockRpc(ulong id, Connected connected, Snapshots snapshots, RpcNet net, RpcConfig rpcConfig) {
        _id = id;
        _connected = connected;
        _snapshots = snapshots;
        _net = net;
        _rpcConfig = rpcConfig;
        _delays = _rpcConfig.NetworkDelay > TimeSpan.Zero;
        _net[id] = this;
        _sameNodePrefix = (1 << sizeof(uint)) - 1;
    }

    public ServerAddressSet KnownPeers { get; } = new();

    public void DelaySendSnapshot(ulong snapshotId) {
        _delaySnapshotId = snapshotId;
    }

    public bool DropPackets() {
        return _rpcConfig.Drops && Random.Shared.Next() % 5 == 0;
    }

    public TimeSpan GetDelay(ulong id) {
        return IsLocalNode(id) ? _rpcConfig.LocalDelay : _rpcConfig.NetworkDelay;
    }

    public bool IsLocalNode(ulong id) {
        return (id & _sameNodePrefix) == (_id & _sameNodePrefix);
    }

    public TimeSpan RandExtraDelay() {
        return TimeSpan.FromMilliseconds(Random.Shared.NextInt64(0, _rpcConfig.ExtraDelayMax.Milliseconds));
    }

    public void ResumeSendSnapshot() {
        _delaySnapshotId = null;
        _delaySnapshot.Release();
    }

    #region IRaftRpcClient Members

    public Task PingAsync(ulong to, DateTime deadline, CancellationToken cancellationToken) {
        throw new NotImplementedException();
    }

    public async Task VoteRequestAsync(ulong to, VoteRequest request) {
        if (!_net.TryGetValue(to, out var toRpc)) {
            throw new UnknownNodeException(to);
        }
        if (_connected.IsConnected(_id, to)) {
            throw new DisconnectedException(_id, to);
        }

        if (DropPackets()) {
            return;
        }

        if (_delays) {
            var delay = GetDelay(to) + RandExtraDelay();
            await Task.Delay(delay);
        }

        if (_connected.IsConnected(_id, to)) {
            await toRpc.HandleVoteRequestAsync(_id, request);
        }
    }

    public async Task VoteResponseAsync(ulong to, VoteResponse response) {
        if (!_net.TryGetValue(to, out var toRpc)) {
            throw new UnknownNodeException(to);
        }
        if (_connected.IsConnected(_id, to)) {
            throw new DisconnectedException(_id, to);
        }

        if (DropPackets()) {
            return;
        }

        if (_delays) {
            var delay = GetDelay(to) + RandExtraDelay();
            await Task.Delay(delay);
        }

        if (_connected.IsConnected(_id, to)) {
            await toRpc.HandleVoteResponseAsync(_id, response);
        }
    }

    public async Task AppendRequestAsync(ulong to, AppendRequest request) {
        if (!_net.TryGetValue(to, out var toRpc)) {
            throw new UnknownNodeException(to);
        }
        if (_connected.IsConnected(_id, to)) {
            throw new DisconnectedException(_id, to);
        }

        if (DropPackets()) {
            return;
        }

        if (_delays) {
            var delay = GetDelay(to) + RandExtraDelay();
            await Task.Delay(delay);
        }

        if (_connected.IsConnected(_id, to)) {
            await toRpc.HandleAppendRequestAsync(_id, request);
        }
    }

    public async Task AppendResponseAsync(ulong to, AppendResponse response) {
        if (!_net.TryGetValue(to, out var toRpc)) {
            throw new UnknownNodeException(to);
        }
        if (_connected.IsConnected(_id, to)) {
            throw new DisconnectedException(_id, to);
        }

        if (DropPackets()) {
            return;
        }

        if (_delays) {
            var delay = GetDelay(to) + RandExtraDelay();
            await Task.Delay(delay);
        }

        if (_connected.IsConnected(_id, to)) {
            await toRpc.HandleAppendResponseAsync(_id, response);
        }
    }

    public async Task<SnapshotResponse> SendSnapshotAsync(ulong to, InstallSnapshotRequest request) {
        if (!_net.TryGetValue(to, out var toRpc)) {
            throw new UnknownNodeException(to);
        }
        if (_connected.IsConnected(_id, to)) {
            throw new DisconnectedException(_id, to);
        }

        // transfer the snapshot
        var snapshotId = request.Snp.Id;
        _snapshots[to][snapshotId] = _snapshots[_id][snapshotId];

        if (_delaySnapshotId != null) {
            await _delaySnapshot.WaitAsync();
            _delaySnapshot.Release(1);
        }

        var response = await toRpc.HandleInstallSnapshotRequestAsync(_id, request);
        return response;
    }

    public async Task TimeoutNowRequestAsync(ulong to, TimeoutNowRequest request) {
        if (!_net.TryGetValue(to, out var toRpc)) {
            throw new UnknownNodeException(to);
        }
        if (_connected.IsConnected(_id, to)) {
            throw new DisconnectedException(_id, to);
        }
        await toRpc.HandleTimeoutNowAsync(_id, request);
    }

    public async Task ReadQuorumRequestAsync(ulong to, ReadQuorumRequest request) {
        if (!_net.TryGetValue(to, out var toRpc)) {
            throw new UnknownNodeException(to);
        }
        if (_connected.IsConnected(_id, to)) {
            throw new DisconnectedException(_id, to);
        }
        if (DropPackets()) {
            return;
        }
        await toRpc.HandleReadQuorumRequestAsync(_id, request);
    }

    public async Task ReadQuorumResponseAsync(ulong to, ReadQuorumResponse response) {
        if (!_net.TryGetValue(to, out var toRpc)) {
            throw new UnknownNodeException(to);
        }
        if (_connected.IsConnected(_id, to)) {
            throw new DisconnectedException(_id, to);
        }
        if (DropPackets()) {
            return;
        }
        await toRpc.HandleReadQuorumResponseAsync(_id, response);
    }

    #endregion

    #region IRaftRpcHandler Members

    public Task HandleVoteRequestAsync(ulong from, VoteRequest message) {
        throw new NotImplementedException();
    }

    public Task HandleVoteResponseAsync(ulong from, VoteResponse message) {
        throw new NotImplementedException();
    }

    public Task HandleAppendRequestAsync(ulong from, AppendRequest message) {
        throw new NotImplementedException();
    }

    public Task HandleAppendResponseAsync(ulong from, AppendResponse message) {
        throw new NotImplementedException();
    }

    public Task HandleReadQuorumRequestAsync(ulong from, ReadQuorumRequest message) {
        throw new NotImplementedException();
    }

    public Task HandleReadQuorumResponseAsync(ulong from, ReadQuorumResponse message) {
        throw new NotImplementedException();
    }

    public Task HandleTimeoutNowAsync(ulong from, TimeoutNowRequest message) {
        throw new NotImplementedException();
    }

    public Task<SnapshotResponse> HandleInstallSnapshotRequestAsync(ulong from, InstallSnapshotRequest message) {
        throw new NotImplementedException();
    }

    public Task<PingResponse> HandlePingRequestAsync(ulong from, PingRequest message) {
        throw new NotImplementedException();
    }

    #endregion
}
