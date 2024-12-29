namespace RaftNET;

public class LogLimiter(int initialCount, int maxCount) {
    private readonly SemaphoreSlim _semaphore = new(initialCount, maxCount);

    private void WaitOne() {
        _semaphore.Wait();
    }

    public void Wait(int n) {
        if (n > maxCount) {
            throw new ArgumentOutOfRangeException(nameof(n));
        }
        for (var i = 0; i < n; i++) {
            WaitOne();
        }
    }

    public void Release(int n) {
        _semaphore.Release(n);
    }
}