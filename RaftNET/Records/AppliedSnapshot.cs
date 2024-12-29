namespace RaftNET.Records;

public record AppliedSnapshot(
    SnapshotDescriptor Snapshot,
    bool IsLocal,
    ulong PreservedLogEntries // Always 0 for non-local snapshots.
);