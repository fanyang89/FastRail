using Grpc.Core;
using Grpc.Net.Client;

namespace RaftNET.Services;

public class RaftGrpcClient {
    private readonly Raft.RaftClient _client;
    private readonly ulong _myId;

    public RaftGrpcClient(ulong myId, string address) {
        _myId = myId;
        var options = new GrpcChannelOptions { Credentials = ChannelCredentials.Insecure };
        var channel = GrpcChannel.ForAddress(address, options);
        _client = new Raft.RaftClient(channel);
    }

    public void Ping(DateTime deadline, CancellationToken cancellationToken) {
        var metadata = new Metadata { { RaftGrpcService.KeyFromId, _myId.ToString() } };
        _client.Ping(new PingRequest(), metadata, deadline, cancellationToken);
    }

    public async Task PingAsync(DateTime deadline, CancellationToken cancellationToken) {
        var metadata = new Metadata { { RaftGrpcService.KeyFromId, _myId.ToString() } };
        await _client.PingAsync(new PingRequest(), metadata, deadline, cancellationToken).ResponseAsync;
    }

    public async Task VoteAsync(VoteRequest request) {
        var metadata = new Metadata { { RaftGrpcService.KeyFromId, _myId.ToString() } };
        await _client.VoteAsync(request, metadata).ResponseAsync;
    }

    public async Task RespondVoteAsync(VoteResponse response) {
        var metadata = new Metadata { { RaftGrpcService.KeyFromId, _myId.ToString() } };
        await _client.RespondVoteAsync(response, metadata).ResponseAsync;
    }

    public async Task AppendAsync(AppendRequest request) {
        var metadata = new Metadata { { RaftGrpcService.KeyFromId, _myId.ToString() } };
        await _client.AppendAsync(request, metadata).ResponseAsync;
    }

    public async Task RespondAppendAsync(AppendResponse response) {
        var metadata = new Metadata { { RaftGrpcService.KeyFromId, _myId.ToString() } };
        await _client.RespondAppendAsync(response, metadata).ResponseAsync;
    }

    public async Task<SnapshotResponse> SendSnapshotAsync(InstallSnapshotRequest request) {
        var metadata = new Metadata { { RaftGrpcService.KeyFromId, _myId.ToString() } };
        return await _client.SendSnapshotAsync(request, metadata).ResponseAsync;
    }

    public async Task TimeoutNowAsync(TimeoutNowRequest request) {
        var metadata = new Metadata { { RaftGrpcService.KeyFromId, _myId.ToString() } };
        await _client.TimeoutNowAsync(request, metadata).ResponseAsync;
    }

    public async Task ReadQuorumAsync(ReadQuorumRequest request) {
        var metadata = new Metadata { { RaftGrpcService.KeyFromId, _myId.ToString() } };
        await _client.ReadQuorumAsync(request, metadata).ResponseAsync;
    }

    public async Task RespondReadQuorumAsync(ReadQuorumResponse response) {
        var metadata = new Metadata { { RaftGrpcService.KeyFromId, _myId.ToString() } };
        await _client.RespondReadQuorumAsync(response, metadata).ResponseAsync;
    }
}
