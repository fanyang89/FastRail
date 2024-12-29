namespace RaftNET.FailureDetectors;

public class SystemClock : IClock {
    public DateTime Now { get; } = DateTime.Now;
}