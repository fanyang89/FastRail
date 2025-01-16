namespace RaftNET.FailureDetectors;

public interface IClock {
    DateTime Now { get; }
    void SleepFor(TimeSpan duration);
}
