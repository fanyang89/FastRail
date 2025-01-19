using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace RaftNET.Services;

public class ConnectionManager(ulong myId, AddressBook addressBook, ILogger<ConnectionManager>? logger = null) : IRaftRpcClient {
    private readonly Dictionary<ulong, RaftGrpcClient> _channels = new();
    private readonly ILogger<ConnectionManager> _logger = logger ?? new NullLogger<ConnectionManager>();

    public async Task PingAsync(ulong to, DateTime deadline, CancellationToken cancellationToken) {
        var conn = EnsureConnection(to);
        await conn.PingAsync(deadline, cancellationToken);
    }

    public async Task VoteRequestAsync(ulong to, VoteRequest request) {
        var conn = EnsureConnection(to);
        _logger.LogTrace("Send({}->{}) VoteRequest={}", myId, to, request);
        await conn.VoteAsync(request);
    }

    public async Task VoteResponseAsync(ulong to, VoteResponse response) {
        var conn = EnsureConnection(to);
        _logger.LogTrace("Send({}->{}) VoteResponse={}", myId, to, response);
        await conn.RespondVoteAsync(response);
    }

    public async Task AppendRequestAsync(ulong to, AppendRequest request) {
        var conn = EnsureConnection(to);
        _logger.LogTrace("Send({}->{}) AppendRequest={}", myId, to, request);
        await conn.AppendAsync(request);
    }

    public async Task AppendResponseAsync(ulong to, AppendResponse response) {
        var conn = EnsureConnection(to);
        _logger.LogTrace("Send({}->{}) AppendResponse={}", myId, to, response);
        await conn.RespondAppendAsync(response);
    }

    public async Task<SnapshotResponse> SendSnapshotAsync(ulong to, InstallSnapshotRequest request) {
        var conn = EnsureConnection(to);
        _logger.LogTrace("Send({}->{}) SendSnapshot={}", myId, to, request);
        var response = await conn.SendSnapshotAsync(request);
        _logger.LogTrace("Send({}->{}) SendSnapshot success, response={}", myId, to, request);
        return response;
    }

    public async Task TimeoutNowRequestAsync(ulong to, TimeoutNowRequest request) {
        var conn = EnsureConnection(to);
        _logger.LogTrace("Send({}->{}) TimeoutNowRequest={}", myId, to, request);
        await conn.TimeoutNowAsync(request);
    }

    public async Task ReadQuorumRequestAsync(ulong to, ReadQuorumRequest request) {
        var conn = EnsureConnection(to);
        _logger.LogTrace("Send({}->{}) ReadQuorumRequest={}", myId, to, request);
        await conn.ReadQuorumAsync(request);
    }

    public async Task ReadQuorumResponseAsync(ulong to, ReadQuorumResponse response) {
        var conn = EnsureConnection(to);
        _logger.LogTrace("Send({}->{}) ReadQuorumResponse={}", myId, to, response);
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
