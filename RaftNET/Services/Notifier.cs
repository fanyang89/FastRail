using System.Collections.Concurrent;

namespace RaftNET.Services;

public class Notifier {
    private readonly BlockingCollection<object> _queue = new();

    public void Signal() {
        _queue.Add(new object());
    }

    public void Wait() {
        _ = _queue.Take();
    }
}
