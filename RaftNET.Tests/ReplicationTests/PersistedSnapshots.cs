namespace RaftNET.Tests.ReplicationTests;

public class PersistedSnapshots : Dictionary<ulong, (SnapshotDescriptor, SnapshotValue)>;
