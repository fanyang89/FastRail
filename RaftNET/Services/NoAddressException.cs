namespace RaftNET.Services;

public class NoAddressException(ulong id) : Exception {
    public override string Message { get; } = $"Can't find address for server {id}";
}