namespace RaftNET.Services;

public interface IRaftClient {
    public Task Ping(DateTime deadline);
    public Task VoteRequest(VoteRequest request);
    public Task VoteResponse(VoteResponse response);
    public Task AppendRequest(AppendRequest request);
    public Task AppendResponse(AppendResponse request);
    public Task InstallSnapshot(InstallSnapshot request);
    public Task SnapshotResponse(SnapshotResponse request);
    public Task TimeoutNowRequest(TimeoutNowRequest request);
}
