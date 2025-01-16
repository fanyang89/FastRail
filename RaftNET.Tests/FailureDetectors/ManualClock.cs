using RaftNET.FailureDetectors;

namespace RaftNET.Tests.FailureDetectors;

public class ManualClock(DateTime now) : IClock {
    public DateTime Now { get; private set; } = now;

    public void SleepFor(TimeSpan duration) {}

    public void Advance(TimeSpan duration) {
        Now += duration;
    }

    public void Set(DateTime dateTime) {
        Now = dateTime;
    }
}
