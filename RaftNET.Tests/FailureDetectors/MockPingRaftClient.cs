using Grpc.Core;
using Microsoft.Extensions.Logging;
using RaftNET.Services;

namespace RaftNET.Tests.FailureDetectors;

class MockPingRaftClient(ulong myId, ILogger<MockPingRaftClient> logger) : IRaftRpcClient {
    private bool _injectedException;

    public Task PingAsync(DateTime deadline) {
        logger.LogInformation("Ping({})", myId);

        if (_injectedException) {
            throw new RpcException(new Status(StatusCode.Unavailable, "injected ping failure"));
        }

        return Task.CompletedTask;
    }

    public Task VoteRequestAsync(VoteRequest request) {
        return Task.CompletedTask;
    }

    public Task VoteResponseAsync(VoteResponse response) {
        return Task.CompletedTask;
    }

    public Task AppendRequestAsync(AppendRequest request) {
        return Task.CompletedTask;
    }

    public Task AppendResponseAsync(AppendResponse request) {
        return Task.CompletedTask;
    }

    public Task<SnapshotResponse> SendSnapshotAsync(InstallSnapshotRequest request) {
        return Task.FromResult(new SnapshotResponse());
    }

    public Task TimeoutNowRequestAsync(TimeoutNowRequest request) {
        return Task.CompletedTask;
    }

    public void InjectPingException() {
        _injectedException = true;
    }
}
