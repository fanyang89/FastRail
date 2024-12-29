using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace RaftNET.Services;

public class ConnectionManager(ulong myId, AddressBook addressBook, ILogger<ConnectionManager> logger) {
    private readonly Dictionary<ulong, RaftClient> _channels = new();

    private RaftClient EnsureConnection(ulong id) {
        if (_channels.TryGetValue(id, out var connection)) {
            return connection;
        } else {
            var addr = addressBook.Find(id);
            if (addr == null) {
                throw new NoAddressException(id);
            } else {
                var client = new RaftClient(myId, addr);
                _channels.Add(id, client);
                return client;
            }
        }
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
        } else if (message.IsInstallSnapshot) {
            logger.LogTrace("Send({}->{}) InstallSnapshot={}", myId, to, message.InstallSnapshot);
            await conn.InstallSnapshot(message.InstallSnapshot);
        } else if (message.IsSnapshotResponse) {
            logger.LogTrace("Send({}->{}) SnapshotResponse={}", myId, to, message.SnapshotResponse);
            await conn.SnapshotResponse(message.SnapshotResponse);
        } else if (message.IsTimeoutNowRequest) {
            logger.LogTrace("Send({}->{}) TimeoutNowRequest={}", myId, to, message.TimeoutNowRequest);
            await conn.TimeoutNowRequest(message.TimeoutNowRequest);
        } else {
            Debug.Assert(false);
        }
    }
}