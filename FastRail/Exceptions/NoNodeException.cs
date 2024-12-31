namespace FastRail.Exceptions;

public class NoNodeException(string key) : Exception {
    public override string Message { get; } = $"Node doesn't exists, key={key}";
}