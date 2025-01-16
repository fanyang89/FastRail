namespace RaftNET.Services;

public class CommandTooLargeException(int length, int maxLength) : Exception {
    public override string Message { get; } = $"Command length {length} exceeded maximum of {maxLength}";
}
