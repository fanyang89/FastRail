namespace RaftNET;

public interface IClock {
    DateTime Now { get; }
}