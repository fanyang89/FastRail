namespace RaftNET;

public class SystemClock : IClock {
    public DateTime Now { get; } = DateTime.Now;
}