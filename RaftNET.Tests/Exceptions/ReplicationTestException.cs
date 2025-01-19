namespace RaftNET.Tests.ReplicationTests;

class ReplicationTestException : Exception {
    public override string Message => "Replication test failed";
}
