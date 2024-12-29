using RaftNET.FailureDetectors;

namespace RaftNET.Tests.FailureDetectors;

public class ManualClock(DateTime now) : IClock {
    private DateTime _now = now;
    public DateTime Now => _now;

    public void SleepFor(TimeSpan duration) {}

    public void Advance(TimeSpan duration) {
        _now += duration;
    }

    public void Set(DateTime dateTime) {
        _now = dateTime;
    }
}