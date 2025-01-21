namespace RaftNET.Exceptions;

public class ElectionTimeoutException(TimeSpan elapsed) : ApplicationException {
    public override string Message => $"Election timeout exceeded, elapsed {elapsed}";
}
