namespace RaftNET.Tests.ReplicationTests;

public record Entries(ulong N, ulong? Server = null, bool Concurrent = false);
