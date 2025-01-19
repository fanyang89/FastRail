namespace RaftNET.Tests.Replications;

public class PersistedSnapshots : Dictionary<int, (SnapshotDescriptor, SnapshotValue)>;