namespace FastRail.Exceptions;

public class RailException(int errorCode) : Exception {
    public int Err => errorCode;
    public override string Message => $"Rail server exception, code={errorCode}";
}