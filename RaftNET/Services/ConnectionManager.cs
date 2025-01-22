using Serilog;

namespace RaftNET.Services;

public class ConnectionManager(ulong myId, AddressBook addressBook) : IRaftRpcClient {
    private readonly Dictionary<ulong, RaftGrpcClient> _channels = new();
    public ulong Id { get; } = myId;

    public async Task AppendRequestAsync(ulong to, AppendRequest request) {
        var conn = EnsureConnection(to);
        Log.Debug("Send({from}->{to}) AppendRequest={request}", Id, to, request);
        await conn.AppendAsync(request);
    }

    public async Task AppendResponseAsync(ulong to, AppendResponse response) {
        var conn = EnsureConnection(to);
        Log.Debug("Send({from}->{to}) AppendResponse={response}", Id, to, response);
        await conn.RespondAppendAsync(response);
    }

    public async Task PingAsync(ulong to, DateTime deadline, CancellationToken cancellationToken) {
        var conn = EnsureConnection(to);
        await conn.PingAsync(deadline, cancellationToken);
    }

    public async Task ReadQuorumRequestAsync(ulong to, ReadQuorumRequest request) {
        var conn = EnsureConnection(to);
        Log.Debug("Send({from}->{to}) ReadQuorumRequest={request}", Id, to, request);
        await conn.ReadQuorumAsync(request);
    }

    public async Task ReadQuorumResponseAsync(ulong to, ReadQuorumResponse response) {
        var conn = EnsureConnection(to);
        Log.Debug("Send({from}->{to}) ReadQuorumResponse={response}", Id, to, response);
        await conn.RespondReadQuorumAsync(response);
    }

    public async Task<SnapshotResponse> SendSnapshotAsync(ulong to, InstallSnapshotRequest request) {
        var conn = EnsureConnection(to);
        Log.Debug("Send({from}->{to}) SendSnapshot={request}", Id, to, request);
        var response = await conn.SendSnapshotAsync(request);
        Log.Debug("Send({from}->{to}) SendSnapshot success, response={response}", Id, to, response);
        return response;
    }

    public async Task TimeoutNowRequestAsync(ulong to, TimeoutNowRequest request) {
        var conn = EnsureConnection(to);
        Log.Debug("Send({from}->{to}) TimeoutNowRequest={request}", Id, to, request);
        await conn.TimeoutNowAsync(request);
    }

    public async Task VoteRequestAsync(ulong to, VoteRequest request) {
        var conn = EnsureConnection(to);
        Log.Debug("Send({from}->{to}) VoteRequest={request}", Id, to, request);
        await conn.VoteAsync(request);
    }

    public async Task VoteResponseAsync(ulong to, VoteResponse response) {
        var conn = EnsureConnection(to);
        Log.Debug("Send({from}->{to}) VoteResponse={response}", Id, to, response);
        await conn.RespondVoteAsync(response);
    }

    public void OnConfigurationChange(ISet<ServerAddress> add, ISet<ServerAddress> del) {}

    private RaftGrpcClient EnsureConnection(ulong id) {
        if (_channels.TryGetValue(id, out var connection)) {
            return connection;
        }

        var addr = addressBook.Find(id);
        if (addr == null) {
            throw new NoAddressException(id);
        }
        var client = new RaftGrpcClient(Id, addr);
        _channels.Add(id, client);
        return client;
    }
}
