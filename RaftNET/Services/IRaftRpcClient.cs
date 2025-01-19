namespace RaftNET.Services;

public interface IRaftRpcClient {
    public Task PingAsync(DateTime deadline);
    public Task VoteRequestAsync(VoteRequest request);
    public Task VoteResponseAsync(VoteResponse response);
    public Task AppendRequestAsync(AppendRequest request);
    public Task AppendResponseAsync(AppendResponse request);
    public Task<SnapshotResponse> SendSnapshotAsync(InstallSnapshotRequest request);
    public Task TimeoutNowRequestAsync(TimeoutNowRequest request);
}
