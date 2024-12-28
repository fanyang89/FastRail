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
                var client = RaftClient.Dial(myId, addr);
                _channels.Add(id, client);
                return client;
            }
        }
    }

    public async Task Send(ulong to, Message message) {
        var conn = EnsureConnection(to);
        if (message.IsVoteRequest) {
            await conn.VoteRequest(message.VoteRequest);
        } else if (message.IsVoteResponse) {
            await conn.VoteResponse(message.VoteResponse);
        } else if (message.IsAppendRequest) {
            await conn.AppendRequest(message.AppendRequest);
        } else if (message.IsAppendResponse) {
            await conn.AppendResponse(message.AppendResponse);
        } else if (message.IsInstallSnapshot) {
            await conn.InstallSnapshot(message.InstallSnapshot);
        } else if (message.IsSnapshotResponse) {
            await conn.SnapshotResponse(message.SnapshotResponse);
        } else if (message.IsTimeoutNowRequest) {
            await conn.TimeoutNowRequest(message.TimeoutNowRequest);
        } else {
            Debug.Assert(false);
        }
    }
}