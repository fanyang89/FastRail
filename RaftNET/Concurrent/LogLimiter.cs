namespace RaftNET.Concurrent;

public class LogLimiter(int initialCount, int maxCount) {
    private readonly object _lock = new();
    private int _count = initialCount;
    private Exception? _lastException;

    public void Broken(Exception ex) {
        lock (_lock) {
            _lastException = ex;
            Monitor.PulseAll(_lock);
        }
    }

    public void Consume(int n) {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(n, maxCount);
        lock (_lock) {
            while (_count < n || _lastException != null) {
                Monitor.Wait(_lock);
            }
            if (_lastException != null) {
                throw _lastException;
            }
            _count -= n;
        }
    }

    public void Signal(int n) {
        lock (_lock) {
            _count += Math.Min(maxCount, _count + n);
            Monitor.Pulse(_lock);
        }
    }
}
