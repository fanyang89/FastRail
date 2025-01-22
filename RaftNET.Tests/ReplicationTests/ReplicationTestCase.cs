using RaftNET.Services;

namespace RaftNET.Tests.ReplicationTests;

public record ReplicationTestCase {
    public readonly Dictionary<ulong, RaftServiceOptions> Config = [];
    public readonly ulong InitialLeader = 1;
    public readonly Dictionary<ulong, SnapshotDescriptor> InitialSnapshots = [];
    public Dictionary<ulong, List<LogEntrySlim>> InitialStates = [];
    public ulong InitialTerm = 1;
    public required ulong Nodes;
    public ulong TotalValues = 100;
    public List<Update> Updates = [];
    public bool VerifyPersistedSnapshots = true;

    public ulong GetFirstValue() {
        ulong firstValue = 0;
        if (InitialLeader < (ulong)InitialStates.Count) {
            firstValue += (ulong)InitialStates[InitialLeader].Count;
        }
        if (InitialLeader < (ulong)InitialSnapshots.Count) {
            firstValue += InitialSnapshots[InitialLeader].Idx;
        }
        return firstValue;
    }
};
