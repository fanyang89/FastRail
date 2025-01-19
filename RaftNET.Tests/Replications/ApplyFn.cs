namespace RaftNET.Tests.Replications;

public delegate int ApplyFn(ulong id, List<Command> commands, HasherInt hasher);