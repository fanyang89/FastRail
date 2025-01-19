namespace RaftNET.Tests.ReplicationTests;

public class ReadValue {
    public ulong NodeIdx; // which node should read?
    public ulong ExpectedIdx; // expected read index
}
