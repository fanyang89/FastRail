namespace FastRail.Server;

public record Session {
    public long SessionID;
    public TimeSpan Timeout;
    public DateTime LastLive;
}