using System.Collections.Concurrent;

namespace RaftNET.Services;

public class Notifier {
    private class Dummy;

    private readonly BlockingCollection<Dummy> _dummies = new();

    public void Signal() {
        _dummies.Add(new Dummy());
    }

    public void Wait() {
        _ = _dummies.Take();
    }
}
