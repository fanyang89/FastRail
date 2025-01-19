using System.Diagnostics;
using Microsoft.Extensions.Logging;
using RaftNET.Records;

namespace RaftNET.Services;

public class ConnectionManager(ulong myId, AddressBook addressBook, ILogger<ConnectionManager> logger) {
    private readonly Dictionary<ulong, IRaftRpcClient> _channels = new();

    public async Task<SnapshotResponse> SendInstallSnapshotRequest(ulong to, InstallSnapshotRequest request) {
        var conn = EnsureConnection(to);
        logger.LogTrace("Send({}->{}) InstallSnapshotRequest={}", myId, to, request);
        return await conn.SendSnapshotAsync(request);
    }

    public async Task Send(ulong to, Message message) {
        var conn = EnsureConnection(to);

        if (message.IsVoteRequest) {
            logger.LogTrace("Send({}->{}) VoteRequest={}", myId, to, message.VoteRequest);
            await conn.VoteRequestAsync(message.VoteRequest);
        } else if (message.IsVoteResponse) {
            logger.LogTrace("Send({}->{}) VoteResponse={}", myId, to, message.VoteResponse);
            await conn.VoteResponseAsync(message.VoteResponse);
        } else if (message.IsAppendRequest) {
            logger.LogTrace("Send({}->{}) AppendRequest={}", myId, to, message.AppendRequest);
            await conn.AppendRequestAsync(message.AppendRequest);
        } else if (message.IsAppendResponse) {
            logger.LogTrace("Send({}->{}) AppendResponse={}", myId, to, message.AppendResponse);
            await conn.AppendResponseAsync(message.AppendResponse);
        } else if (message.IsTimeoutNowRequest) {
            logger.LogTrace("Send({}->{}) TimeoutNowRequest={}", myId, to, message.TimeoutNowRequest);
            await conn.TimeoutNowRequestAsync(message.TimeoutNowRequest);
        } else {
            throw new UnreachableException();
        }
    }

    private IRaftRpcClient EnsureConnection(ulong id) {
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
