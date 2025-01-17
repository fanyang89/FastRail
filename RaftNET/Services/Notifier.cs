using System.Collections.Concurrent;

namespace RaftNET.Services;

public class Notifier {
    private class Bottom;

    private readonly BlockingCollection<Bottom> _queue = new();

    public void Signal() {
        _queue.Add(new Bottom());
    }

    public void Wait() {
        _ = _queue.Take();
    }
}
