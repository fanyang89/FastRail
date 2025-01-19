namespace RaftNET.Tests.ReplicationTests;

public record Entries(int N, int? Server = null, bool Concurrent = false);
