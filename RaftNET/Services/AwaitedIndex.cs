namespace RaftNET.Services;

record AwaitedIndex {
    public TaskCompletionSource<ReadBarrierResponse> Promise = new();
    public CancellationToken? Abort;
}