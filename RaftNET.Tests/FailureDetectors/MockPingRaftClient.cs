using Grpc.Core;
using Microsoft.Extensions.Logging;
using RaftNET.Services;

namespace RaftNET.Tests.FailureDetectors;

internal class MockPingRaftClient(ulong myId, ILogger<MockPingRaftClient> logger) : IRaftClient {
    private bool _injectedException;

    public Task Ping(DateTime deadline) {
        logger.LogInformation("Ping({})", myId);

        if (_injectedException) {
            throw new RpcException(new Status(StatusCode.Unavailable, "injected ping failure"));
        }

        return Task.CompletedTask;
    }

    public void InjectPingException() {
        _injectedException = true;
    }

    public Task VoteRequest(VoteRequest request) {
        return Task.CompletedTask;
    }

    public Task VoteResponse(VoteResponse response) {
        return Task.CompletedTask;
    }

    public Task AppendRequest(AppendRequest request) {
        return Task.CompletedTask;
    }

    public Task AppendResponse(AppendResponse request) {
        return Task.CompletedTask;
    }

    public Task InstallSnapshot(InstallSnapshot request) {
        return Task.CompletedTask;
    }

    public Task SnapshotResponse(SnapshotResponse request) {
        return Task.CompletedTask;
    }

    public Task TimeoutNowRequest(TimeoutNowRequest request) {
        return Task.CompletedTask;
    }
}