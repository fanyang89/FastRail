using System.Diagnostics;
using Grpc.Core;
using Microsoft.Extensions.Hosting;

namespace RaftNET.Services;

public class RaftGrpcService(RaftService handler) : Raft.RaftBase, IHostedService {
    public const string KeyFromId = "raftnet-from-id";

    public Task StartAsync(CancellationToken cancellationToken) {
        handler.Start(cancellationToken);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) {
        return handler.StopAsync(cancellationToken);
    }

    public override async Task<Void> Append(AppendRequest request, ServerCallContext context) {
        var from = GetFromServerId(context.RequestHeaders);
        await handler.HandleAppendRequestAsync(from, request);
        return new Void();
    }

    public override async Task<PingResponse> Ping(PingRequest request, ServerCallContext context) {
        var from = GetFromServerId(context.RequestHeaders);
        return await handler.HandlePingRequestAsync(from, request);
    }

    public override async Task<Void> ReadQuorum(ReadQuorumRequest request, ServerCallContext context) {
        var from = GetFromServerId(context.RequestHeaders);
        await handler.HandleReadQuorumRequestAsync(from, request);
        return new Void();
    }

    public override async Task<Void> RespondAppend(AppendResponse request, ServerCallContext context) {
        var from = GetFromServerId(context.RequestHeaders);
        await handler.HandleAppendResponseAsync(from, request);
        return new Void();
    }

    public override async Task<Void> RespondReadQuorum(ReadQuorumResponse request, ServerCallContext context) {
        var from = GetFromServerId(context.RequestHeaders);
        await handler.HandleReadQuorumResponseAsync(from, request);
        return new Void();
    }

    public override async Task<Void> RespondVote(VoteResponse request, ServerCallContext context) {
        var from = GetFromServerId(context.RequestHeaders);
        await handler.HandleVoteResponseAsync(from, request);
        return new Void();
    }

    public override async Task<SnapshotResponse> SendSnapshot(InstallSnapshotRequest request, ServerCallContext context) {
        var from = GetFromServerId(context.RequestHeaders);
        return await handler.HandleInstallSnapshotRequestAsync(from, request);
    }

    public override async Task<Void> TimeoutNow(TimeoutNowRequest request, ServerCallContext context) {
        var from = GetFromServerId(context.RequestHeaders);
        await handler.HandleTimeoutNowAsync(from, request);
        return new Void();
    }

    public override async Task<Void> Vote(VoteRequest request, ServerCallContext context) {
        var from = GetFromServerId(context.RequestHeaders);
        await handler.HandleVoteRequestAsync(from, request);
        return new Void();
    }

    private static ulong GetFromServerId(Metadata metadata) {
        Debug.Assert(metadata.Any(x => x.Key == KeyFromId));
        var entry = metadata.First(x => x.Key == KeyFromId);
        return Convert.ToUInt64(entry.Value);
    }
}
