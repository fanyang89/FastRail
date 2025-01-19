namespace RaftNET.Tests.ReplicationTests;

public record Disconnect(ulong First, ulong Second) : TwoNodes(First, Second) {}
