namespace RaftNET.Tests.ReplicationTests;

public record SnapshotValue {
    public HasherInt Hasher = new(false);
    public ulong Idx;
}
