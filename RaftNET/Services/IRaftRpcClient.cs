namespace RaftNET.Services;

public interface IRaftRpcClient {
    public Task AppendRequestAsync(ulong to, AppendRequest request);
    public Task AppendResponseAsync(ulong to, AppendResponse request);
    public Task PingAsync(ulong to, DateTime deadline, CancellationToken cancellationToken);
    public Task ReadQuorumRequestAsync(ulong to, ReadQuorumRequest request);
    public Task ReadQuorumResponseAsync(ulong to, ReadQuorumResponse response);
    public Task<SnapshotResponse> SendSnapshotAsync(ulong to, InstallSnapshotRequest request);
    public Task TimeoutNowRequestAsync(ulong to, TimeoutNowRequest request);
    public Task VoteRequestAsync(ulong to, VoteRequest request);
    public Task VoteResponseAsync(ulong to, VoteResponse response);
}
