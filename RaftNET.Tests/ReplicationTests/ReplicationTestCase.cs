using RaftNET.Services;

namespace RaftNET.Tests.ReplicationTests;

public record ReplicationTestCase {
    public required ulong Nodes;
    public ulong TotalValues = 100;
    public ulong InitialTerm = 1;
    public ulong InitialLeader = 0;
    public List<List<LogEntrySlim>> InitialStates = [];
    public List<SnapshotDescriptor> InitialSnapshots = [];
    public List<RaftServiceOptions> Config = [];
    public List<Update> Updates = [];
    public bool CommutativeHash = false;
    public bool VerifyPersistedSnapshots = true;

    public ulong GetFirstValue() {
        ulong firstValue = 0;
        if (InitialLeader < (ulong)InitialStates.Count) {
            firstValue += (ulong)InitialStates[(int)InitialLeader].Count;
        }
        if (InitialLeader < (ulong)InitialSnapshots.Count) {
            firstValue += InitialSnapshots[(int)InitialLeader].Idx;
        }
        return firstValue;
    }
};
