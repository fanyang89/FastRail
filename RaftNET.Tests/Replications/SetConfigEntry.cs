namespace RaftNET.Tests.Replications;

public record SetConfigEntry(ulong NodeIdx, bool CanVote);