using OneOf;

namespace RaftNET.Tests.ReplicationTests;

public record LogEntrySlim(ulong Term, OneOf<int, Configuration> Data);
