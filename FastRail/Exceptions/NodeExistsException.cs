namespace FastRail.Exceptions;

public class NodeExistsException(string key) : Exception {
    public override string Message { get; } = $"Node already exists, key={key}";
}