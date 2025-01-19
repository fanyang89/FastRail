namespace RaftNET.Tests.ReplicationTests;

class DisconnectedException(ulong from, ulong to) : ReplicationTestException {
    public override string Message => $"Disconnected between two servers, from={from} to={to}";
}
