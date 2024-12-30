namespace FastRail.Server;

public record Session {
    public long SessionID;
    public int Timeout;
    public bool IsClosing;
}