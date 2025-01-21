namespace RaftNET.Tests.Exceptions;

class ReplicationTestException : Exception {
    public override string Message => "Replication test failed";
}
