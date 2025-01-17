namespace RaftNET.Exceptions;

public class OutsideConfigurationException(ulong requester) : ApplicationException {
    public override string Message { get; } = $"Read barrier requested by a node outside of the configuration {requester}";
}
