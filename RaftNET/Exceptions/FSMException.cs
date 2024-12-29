namespace RaftNET;

public class FSMException(string message) : Exception {
    public override string Message { get; } = message;
}