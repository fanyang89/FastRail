namespace RaftNET.Services;

record AwaitedConfigurationChange {
    public TaskCompletionSource<ReadBarrierResponse> Promise = new();
    public CancellationToken? Abort;
}