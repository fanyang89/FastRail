using Grpc.Core;
using Grpc.Net.Client;

namespace RaftNET.Services;

public class RaftClient : IRaftRpcClient {
    private readonly Raft.RaftClient _client;
    private readonly ulong _myId;

    public RaftClient(ulong myId, string address) {
        _myId = myId;
        var options = new GrpcChannelOptions {
            // Credentials = ChannelCredentials.Insecure
        };
        var channel = GrpcChannel.ForAddress(address, options);
        _client = new Raft.RaftClient(channel);
    }

    public async Task Ping(DateTime deadline) {
        var metadata = new Metadata { { RaftGrpcService.KeyFromId, _myId.ToString() } };
        await _client.PingAsync(new PingRequest(), metadata, deadline).ResponseAsync;
    }

    public async Task VoteRequest(VoteRequest request) {
        var metadata = new Metadata { { RaftGrpcService.KeyFromId, _myId.ToString() } };
        await _client.VoteAsync(request, metadata).ResponseAsync;
    }

    public async Task VoteResponse(VoteResponse response) {
        var metadata = new Metadata { { RaftGrpcService.KeyFromId, _myId.ToString() } };
        await _client.RespondVoteAsync(response, metadata).ResponseAsync;
    }

    public async Task AppendRequest(AppendRequest request) {
        var metadata = new Metadata { { RaftGrpcService.KeyFromId, _myId.ToString() } };
        await _client.AppendAsync(request, metadata).ResponseAsync;
    }

    public async Task AppendResponse(AppendResponse request) {
        var metadata = new Metadata { { RaftGrpcService.KeyFromId, _myId.ToString() } };
        await _client.RespondAppendAsync(request, metadata).ResponseAsync;
    }

    public async Task<SnapshotResponse> SendSnapshot(InstallSnapshotRequest request) {
        var metadata = new Metadata { { RaftGrpcService.KeyFromId, _myId.ToString() } };
        return await _client.SendSnapshotAsync(request, metadata).ResponseAsync;
    }

    public async Task TimeoutNowRequest(TimeoutNowRequest request) {
        var metadata = new Metadata { { RaftGrpcService.KeyFromId, _myId.ToString() } };
        await _client.TimeoutNowAsync(request, metadata).ResponseAsync;
    }

    public async Task Ping() {
        var metadata = new Metadata { { RaftGrpcService.KeyFromId, _myId.ToString() } };
        await _client.PingAsync(new PingRequest(), metadata).ResponseAsync;
    }
}
