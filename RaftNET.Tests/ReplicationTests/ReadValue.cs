namespace RaftNET.Tests.ReplicationTests;

public class ReadValue {
    public ulong ExpectedIdx; // expected read index
    public ulong NodeIdx; // which node should read?
}
