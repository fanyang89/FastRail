namespace RaftNET.Tests.ReplicationTests;

public record Disconnect(int First, int Second) : TwoNodes(First, Second) {}
