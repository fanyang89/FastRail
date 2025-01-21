namespace RaftNET.Concurrent;

internal class SemaphoreGuard : IDisposable {
    private readonly int _n;
    private readonly SemaphoreSlim _semaphore;

    public SemaphoreGuard(SemaphoreSlim semaphore, int n = 1) {
        _semaphore = semaphore;
        _n = n;

        for (var i = 0; i < n; i++) _semaphore.Wait();
    }

    public void Dispose() {
        _semaphore.Release(_n);
    }
}
