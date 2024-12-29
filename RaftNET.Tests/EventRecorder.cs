namespace RaftNET.Tests;

public class EventRecorder {
    private readonly List<int> _events = [];

    public void Add(int e, string message = "") {
        lock (_events) {
            _events.Add(e);
        }
    }

    public List<int> GetEvents() {
        lock (_events) {
            return [.._events];
        }
    }
}