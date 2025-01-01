namespace FastRail.Server;

public static class Time {
    private static readonly DateTime Jan1st1970 = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    public static long CurrentTimeMillis() {
        return (long)(DateTime.UtcNow - Jan1st1970).TotalMilliseconds;
    }
}