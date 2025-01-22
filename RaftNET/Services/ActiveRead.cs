namespace RaftNET.Services;

record ActiveRead {
    public required ulong Id;
    public required ulong Idx;
    public TaskCompletionSource<ReadBarrierResponse> Promise = new();
    public CancellationToken? Abort;
}