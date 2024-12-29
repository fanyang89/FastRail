namespace RaftNET.FailureDetectors;

public class ManualClock : IClock {
    private DateTime _now;
    public DateTime Now => _now;

    public void Advance(TimeSpan duration) {
        _now += duration;
    }

    public void Set(DateTime dateTime) {
        _now = dateTime;
    }
}