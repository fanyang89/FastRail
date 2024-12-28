using Grpc.Core;
using Grpc.Net.Client;

namespace RaftNET.Services;

public class RaftClient {
    private readonly Raft.RaftClient _client;
    private readonly GrpcChannel _channel;
    private readonly ulong _myId;

    private RaftClient(ulong myId, GrpcChannel channel, Raft.RaftClient client) {
        _client = client;
        _channel = channel;
        _myId = myId;
    }

    public static RaftClient Dial(ulong myId, string address) {
        var channel = GrpcChannel.ForAddress(address);
        var client = new Raft.RaftClient(channel);
        return new RaftClient(myId, channel, client);
    }

    public async Task VoteRequest(VoteRequest request) {
        var metadata = new Metadata {
            {
                RaftService.KeyFromId, _myId.ToString()
            }
        };
        await _client.VoteAsync(request, metadata).ResponseAsync;
    }

    public async Task VoteResponse(VoteResponse response) {
        var metadata = new Metadata {
            {
                RaftService.KeyFromId, _myId.ToString()
            }
        };
        await _client.RespondVoteAsync(response, metadata).ResponseAsync;
    }

    public async Task AppendRequest(AppendRequest request) {
        var metadata = new Metadata {
            {
                RaftService.KeyFromId, _myId.ToString()
            }
        };
        await _client.AppendAsync(request, metadata).ResponseAsync;
    }

    public async Task AppendResponse(AppendResponse request) {
        var metadata = new Metadata {
            {
                RaftService.KeyFromId, _myId.ToString()
            }
        };
        await _client.RespondAppendAsync(request, metadata).ResponseAsync;
    }

    public async Task InstallSnapshot(InstallSnapshot request) {
        var metadata = new Metadata {
            {
                RaftService.KeyFromId, _myId.ToString()
            }
        };
        await _client.SendSnapshotAsync(request, metadata).ResponseAsync;
    }

    public async Task SnapshotResponse(SnapshotResponse request) {
        var metadata = new Metadata {
            {
                RaftService.KeyFromId, _myId.ToString()
            }
        };
        await _client.RespondSendSnapshotAsync(request, metadata).ResponseAsync;
    }

    public async Task TimeoutNowRequest(TimeoutNowRequest request) {
        var metadata = new Metadata {
            {
                RaftService.KeyFromId, _myId.ToString()
            }
        };
        await _client.TimeoutNowAsync(request, metadata).ResponseAsync;
    }
}