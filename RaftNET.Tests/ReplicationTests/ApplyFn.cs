namespace RaftNET.Tests.ReplicationTests;

public delegate int ApplyFn(ulong id, List<Command> commands, HasherInt hasher);
