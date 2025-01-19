namespace RaftNET.Services;

public interface IRaftRpcClient {
    public Task Ping(DateTime deadline);
    public Task VoteRequest(VoteRequest request);
    public Task VoteResponse(VoteResponse response);
    public Task AppendRequest(AppendRequest request);
    public Task AppendResponse(AppendResponse request);
    public Task<SnapshotResponse> SendSnapshot(InstallSnapshotRequest request);
    public Task TimeoutNowRequest(TimeoutNowRequest request);
}
