namespace RaftNET.Tests.Replications;

public record Entries(int N, int? Server = null, bool Concurrent = false);
