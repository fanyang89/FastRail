using Serilog;

namespace RaftNET.Services;

public class ConnectionManager(ulong myId, AddressBook addressBook) : IRaftRpcClient {
    private readonly Dictionary<ulong, RaftGrpcClient> _channels = new();

    public async Task PingAsync(ulong to, DateTime deadline, CancellationToken cancellationToken) {
        var conn = EnsureConnection(to);
        await conn.PingAsync(deadline, cancellationToken);
    }

    public async Task VoteRequestAsync(ulong to, VoteRequest request) {
        var conn = EnsureConnection(to);
        Log.Debug("Send({}->{}) VoteRequest={}", myId, to, request);
        await conn.VoteAsync(request);
    }

    public async Task VoteResponseAsync(ulong to, VoteResponse response) {
        var conn = EnsureConnection(to);
        Log.Debug("Send({}->{}) VoteResponse={}", myId, to, response);
        await conn.RespondVoteAsync(response);
    }

    public async Task AppendRequestAsync(ulong to, AppendRequest request) {
        var conn = EnsureConnection(to);
        Log.Debug("Send({}->{}) AppendRequest={}", myId, to, request);
        await conn.AppendAsync(request);
    }

    public async Task AppendResponseAsync(ulong to, AppendResponse response) {
        var conn = EnsureConnection(to);
        Log.Debug("Send({}->{}) AppendResponse={}", myId, to, response);
        await conn.RespondAppendAsync(response);
    }

    public async Task<SnapshotResponse> SendSnapshotAsync(ulong to, InstallSnapshotRequest request) {
        var conn = EnsureConnection(to);
        Log.Debug("Send({}->{}) SendSnapshot={}", myId, to, request);
        var response = await conn.SendSnapshotAsync(request);
        Log.Debug("Send({}->{}) SendSnapshot success, response={}", myId, to, response);
        return response;
    }

    public async Task TimeoutNowRequestAsync(ulong to, TimeoutNowRequest request) {
        var conn = EnsureConnection(to);
        Log.Debug("Send({}->{}) TimeoutNowRequest={}", myId, to, request);
        await conn.TimeoutNowAsync(request);
    }

    public async Task ReadQuorumRequestAsync(ulong to, ReadQuorumRequest request) {
        var conn = EnsureConnection(to);
        Log.Debug("Send({}->{}) ReadQuorumRequest={}", myId, to, request);
        await conn.ReadQuorumAsync(request);
    }

    public async Task ReadQuorumResponseAsync(ulong to, ReadQuorumResponse response) {
        var conn = EnsureConnection(to);
        Log.Debug("Send({}->{}) ReadQuorumResponse={}", myId, to, response);
        await conn.RespondReadQuorumAsync(response);
    }

    private RaftGrpcClient EnsureConnection(ulong id) {
        if (_channels.TryGetValue(id, out var connection)) {
            return connection;
        }

        var addr = addressBook.Find(id);
        if (addr == null) {
            throw new NoAddressException(id);
        }
        var client = new RaftGrpcClient(myId, addr);
        _channels.Add(id, client);
        return client;
    }
}
