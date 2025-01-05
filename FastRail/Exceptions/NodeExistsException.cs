namespace FastRail.Exceptions;

public class NodeExistsException(string key) : RailException(ErrorCodes.NodeExists) {
    public override string Message { get; } = $"Node already exists, key={key}";
}