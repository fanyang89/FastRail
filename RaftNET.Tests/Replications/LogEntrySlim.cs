using OneOf;

namespace RaftNET.Tests.Replications;

public record LogEntrySlim(ulong Term, OneOf<int, Configuration> Data);