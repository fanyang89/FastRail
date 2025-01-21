namespace FastRail.Server;

public record Session {
    public DateTime LastLive;
    public long SessionID;
    public TimeSpan Timeout;
}
