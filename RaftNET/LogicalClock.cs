namespace RaftNET;

public class LogicalClock {
    private long _now;

    public void Advance(LogicalDuration duration) {
        _now += duration.Value;
    }

    public LogicalDuration Now() {
        return new LogicalDuration(_now);
    }
}