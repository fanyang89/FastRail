namespace FastRail.Server;

public static class Time {
    private static readonly DateTime TimestampOrigin = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    public static long CurrentTimeMillis() {
        return (long)(DateTime.UtcNow - TimestampOrigin).TotalMilliseconds;
    }
}