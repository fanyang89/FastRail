namespace RaftNET.Tests.Replications;

public record SnapshotValue {
    public HasherInt Hasher = new(false);
    public int Idx;
}
