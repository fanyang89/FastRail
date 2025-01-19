namespace RaftNET.Tests.ReplicationTests;

public class PersistedSnapshots : Dictionary<int, (SnapshotDescriptor, SnapshotValue)>;
