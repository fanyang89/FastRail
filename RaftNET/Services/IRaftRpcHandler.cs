namespace RaftNET.Services;

public interface IRaftRpcHandler {
    public Task HandleVoteRequest(ulong from, VoteRequest message);
    public Task HandleVoteResponse(ulong from, VoteResponse message);
    public Task HandleAppendRequest(ulong from, AppendRequest message);
    public Task HandleAppendResponse(ulong from, AppendResponse message);
    public Task HandleReadQuorumRequest(ulong from, ReadQuorumRequest message);
    public Task HandleReadQuorumResponse(ulong from, ReadQuorumResponse message);
    public Task HandleTimeoutNow(ulong from, TimeoutNowRequest message);
    public Task<SnapshotResponse> HandleInstallSnapshotRequest(ulong from, InstallSnapshotRequest message);
    public Task<PingResponse> HandlePingRequest(ulong from, PingRequest message);
}
