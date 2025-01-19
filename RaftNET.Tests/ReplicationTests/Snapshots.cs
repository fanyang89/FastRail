namespace RaftNET.Tests.ReplicationTests;

public class Snapshots : Dictionary<ulong, Dictionary<ulong, SnapshotValue>>;
