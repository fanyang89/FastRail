namespace RaftNET.Tests.ReplicationTests;

public record SetConfigEntry(ulong NodeIdx, bool CanVote);
