namespace RaftNET.Services;

public class DroppedEntryException : Exception {
    public override string Message { get; } = "Entry dropped";
}
