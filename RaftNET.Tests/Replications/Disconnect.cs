namespace RaftNET.Tests.Replications;

public record Disconnect(int First, int Second) : TwoNodes(First, Second) {}
