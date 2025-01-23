namespace RaftNET.Exceptions;

public class StoppedException(string reason = "") : ApplicationException {
    public override string Message => string.IsNullOrEmpty(reason)
        ? "Raft instance is stopped"
        : $"Raft instance is stopped, reason: {reason}";
}
