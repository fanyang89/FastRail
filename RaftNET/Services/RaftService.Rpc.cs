using System.Diagnostics;
using Grpc.Core;

namespace RaftNET.Services;

public partial class RaftService {
    public static readonly string KeyFromId = "raftnet-from-id";

    private ulong GetFromServerId(Metadata metadata) {
        Debug.Assert(metadata.Any(x => x.Key == KeyFromId));
        var entry = metadata.First(x => x.Key == KeyFromId);
        return Convert.ToUInt64(entry.Value);
    }

    public override Task<Void> Vote(VoteRequest request, ServerCallContext context) {
        var from = GetFromServerId(context.RequestHeaders);

        lock (_fsm) {
            _fsm.Step(from, request);
        }

        return Task.FromResult(new Void());
    }

    public override Task<Void> RespondVote(VoteResponse request, ServerCallContext context) {
        var from = GetFromServerId(context.RequestHeaders);

        lock (_fsm) {
            _fsm.Step(from, request);
        }

        return Task.FromResult(new Void());
    }

    public override Task<Void> Append(AppendRequest request, ServerCallContext context) {
        var from = GetFromServerId(context.RequestHeaders);

        lock (_fsm) {
            _fsm.Step(from, request);
        }

        return Task.FromResult(new Void());
    }

    public override Task<Void> RespondAppend(AppendResponse request, ServerCallContext context) {
        var from = GetFromServerId(context.RequestHeaders);

        lock (_fsm) {
            _fsm.Step(from, request);
        }

        return Task.FromResult(new Void());
    }

    public override Task<Void> SendSnapshot(InstallSnapshot request, ServerCallContext context) {
        var from = GetFromServerId(context.RequestHeaders);

        lock (_fsm) {
            _fsm.Step(from, request);
        }

        return Task.FromResult(new Void());
    }

    public override Task<Void> RespondSendSnapshot(SnapshotResponse request, ServerCallContext context) {
        var from = GetFromServerId(context.RequestHeaders);

        lock (_fsm) {
            _fsm.Step(from, request);
        }

        return Task.FromResult(new Void());
    }

    public override Task<Void> TimeoutNow(TimeoutNowRequest request, ServerCallContext context) {
        var from = GetFromServerId(context.RequestHeaders);

        lock (_fsm) {
            _fsm.Step(from, request);
        }

        return Task.FromResult(new Void());
    }

    public override Task<PingResponse> Ping(PingRequest request, ServerCallContext context) {
        return Task.FromResult(new PingResponse());
    }
}