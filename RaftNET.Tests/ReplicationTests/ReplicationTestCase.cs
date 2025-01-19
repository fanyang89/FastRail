using RaftNET.Services;

namespace RaftNET.Tests.ReplicationTests;

public record ReplicationTestCase {
    public required int Nodes;
    public int TotalValues = 100;
    public ulong InitialTerm = 1;
    public int InitialLeader = 0;
    public List<List<LogEntrySlim>> InitialStates = [];
    public List<SnapshotDescriptor> InitialSnapshots = [];
    public List<RaftServiceOptions> Config = [];
    public List<Update> Updates = [];
    public bool CommutativeHash = false;
    public bool VerifyPersistedSnapshots = true;

    public int GetFirstValue() {
        throw new NotImplementedException();
    }
};
