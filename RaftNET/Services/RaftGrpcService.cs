using System.Diagnostics;
using Grpc.Core;
using Microsoft.Extensions.Hosting;

namespace RaftNET.Services;

public class RaftGrpcService(RaftService handler) : Raft.RaftBase, IHostedService {
    public const string KeyFromId = "raftnet-from-id";

    public Task StartAsync(CancellationToken cancellationToken) {
        return handler.StartAsync(cancellationToken);
    }

    public Task StopAsync(CancellationToken cancellationToken) {
        return handler.StopAsync(cancellationToken);
    }

    private static ulong GetFromServerId(Metadata metadata) {
        Debug.Assert(metadata.Any(x => x.Key == KeyFromId));
        var entry = metadata.First(x => x.Key == KeyFromId);
        return Convert.ToUInt64(entry.Value);
    }

    public override Task<Void> Vote(VoteRequest request, ServerCallContext context) {
        var from = GetFromServerId(context.RequestHeaders);
        handler.HandleVoteRequest(from, request);
        return Task.FromResult(new Void());
    }

    public override Task<Void> RespondVote(VoteResponse request, ServerCallContext context) {
        var from = GetFromServerId(context.RequestHeaders);
        handler.HandleVoteResponse(from, request);
        return Task.FromResult(new Void());
    }

    public override Task<Void> Append(AppendRequest request, ServerCallContext context) {
        var from = GetFromServerId(context.RequestHeaders);
        handler.HandleAppendRequest(from, request);
        return Task.FromResult(new Void());
    }

    public override Task<Void> RespondAppend(AppendResponse request, ServerCallContext context) {
        var from = GetFromServerId(context.RequestHeaders);
        handler.HandleAppendResponse(from, request);
        return Task.FromResult(new Void());
    }

    public override Task<Void> ReadQuorum(ReadQuorumRequest request, ServerCallContext context) {
        var from = GetFromServerId(context.RequestHeaders);
        handler.HandleReadQuorumRequest(from, request);
        return Task.FromResult(new Void());
    }

    public override Task<Void> RespondReadQuorum(ReadQuorumResponse request, ServerCallContext context) {
        var from = GetFromServerId(context.RequestHeaders);
        handler.HandleReadQuorumResponse(from, request);
        return Task.FromResult(new Void());
    }

    public override Task<Void> TimeoutNow(TimeoutNowRequest request, ServerCallContext context) {
        var from = GetFromServerId(context.RequestHeaders);
        handler.HandleTimeoutNow(from, request);
        return Task.FromResult(new Void());
    }

    public override Task<SnapshotResponse> SendSnapshot(InstallSnapshotRequest request, ServerCallContext context) {
        var from = GetFromServerId(context.RequestHeaders);
        return handler.HandleInstallSnapshotRequest(from, request);
    }

    public override Task<PingResponse> Ping(PingRequest request, ServerCallContext context) {
        var from = GetFromServerId(context.RequestHeaders);
        return handler.HandlePingRequest(from, request);
    }
}
