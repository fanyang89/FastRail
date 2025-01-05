namespace FastRail.Exceptions;

public class SystemException(string message) : RailException(ErrorCodes.SystemError) {
    public override string Message { get; } = message;
}