using RaftNET.Concurrent;

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

public class LogLimiterTest {
    [Test]
    public void WaitAndRelease() {
        var recorder = new EventRecorder();
        var limiter = new LogLimiter(0, 10);

        var t1 = new Thread(() => {
            recorder.Add(1);
            limiter.Wait(10);
            recorder.Add(2);
        });
        t1.Start();

        recorder.Add(3);
        limiter.Release(10);
        recorder.Add(4);

        t1.Join();

        var events = recorder.GetEvents();
        Assert.That(events.Count, Is.EqualTo(4));
        Assert.That(recorder.GetEvents(),
            events.First() == 3 ? Is.EqualTo(new List<int>([3, 1, 4, 2])) : Is.EqualTo(new List<int>([1, 3, 4, 2])));
    }
}