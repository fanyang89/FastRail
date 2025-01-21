namespace RaftNET.Services;

public interface IRaftRpcHandler {
    public Task HandleAppendRequestAsync(ulong from, AppendRequest message);
    public Task HandleAppendResponseAsync(ulong from, AppendResponse message);
    public Task<SnapshotResponse> HandleInstallSnapshotRequestAsync(ulong from, InstallSnapshotRequest message);
    public Task<PingResponse> HandlePingRequestAsync(ulong from, PingRequest message);
    public Task HandleReadQuorumRequestAsync(ulong from, ReadQuorumRequest message);
    public Task HandleReadQuorumResponseAsync(ulong from, ReadQuorumResponse message);
    public Task HandleTimeoutNowAsync(ulong from, TimeoutNowRequest message);
    public Task HandleVoteRequestAsync(ulong from, VoteRequest message);
    public Task HandleVoteResponseAsync(ulong from, VoteResponse message);
}
