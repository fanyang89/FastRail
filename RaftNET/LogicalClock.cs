namespace RaftNET;

public class LogicalClock {
    private long _now;

    public void Advance(long duration = 1) {
        _now += duration;
    }

    public long Now() {
        return _now;
    }
}
