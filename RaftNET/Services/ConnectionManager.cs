using System.Diagnostics;
using Microsoft.Extensions.Logging;
using RaftNET.Records;

namespace RaftNET.Services;

public class ConnectionManager(ulong myId, AddressBook addressBook, ILogger<ConnectionManager> logger) {
    private readonly Dictionary<ulong, RaftClient> _channels = new();

    public async Task<SnapshotResponse> SendInstallSnapshotRequest(ulong to, InstallSnapshotRequest request) {
        var conn = EnsureConnection(to);
        logger.LogTrace("Send({}->{}) InstallSnapshotRequest={}", myId, to, request);
        return await conn.SendSnapshot(request);
    }

    public async Task Send(ulong to, Message message) {
        var conn = EnsureConnection(to);

        if (message.IsVoteRequest) {
            logger.LogTrace("Send({}->{}) VoteRequest={}", myId, to, message.VoteRequest);
            await conn.VoteRequest(message.VoteRequest);
        } else if (message.IsVoteResponse) {
            logger.LogTrace("Send({}->{}) VoteResponse={}", myId, to, message.VoteResponse);
            await conn.VoteResponse(message.VoteResponse);
        } else if (message.IsAppendRequest) {
            logger.LogTrace("Send({}->{}) AppendRequest={}", myId, to, message.AppendRequest);
            await conn.AppendRequest(message.AppendRequest);
        } else if (message.IsAppendResponse) {
            logger.LogTrace("Send({}->{}) AppendResponse={}", myId, to, message.AppendResponse);
            await conn.AppendResponse(message.AppendResponse);
        } else if (message.IsTimeoutNowRequest) {
            logger.LogTrace("Send({}->{}) TimeoutNowRequest={}", myId, to, message.TimeoutNowRequest);
            await conn.TimeoutNowRequest(message.TimeoutNowRequest);
        } else {
            throw new UnreachableException();
        }
    }

    private RaftClient EnsureConnection(ulong id) {
        if (_channels.TryGetValue(id, out var connection)) {
            return connection;
        }
        var addr = addressBook.Find(id);

        if (addr == null) {
            throw new NoAddressException(id);
        }
        var client = new RaftClient(myId, addr);
        _channels.Add(id, client);
        return client;
    }
}
