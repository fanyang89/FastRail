namespace RaftNET.FailureDetectors;

public class SystemClock : IClock {
    public DateTime Now { get; } = DateTime.Now;

    public void SleepFor(TimeSpan duration) {
        Thread.Sleep(duration);
    }
}
