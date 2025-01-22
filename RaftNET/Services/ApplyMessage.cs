using OneOf;

namespace RaftNET.Services;

public class ApplyMessage : OneOfBase<Exiting, List<LogEntry>, SnapshotDescriptor, RemovedFromConfig> {
    public ApplyMessage(Exiting exit) : base(exit) {}
    public ApplyMessage(List<LogEntry> entries) : base(entries) {}
    public ApplyMessage(SnapshotDescriptor snapshotDescriptor) : base(snapshotDescriptor) {}
    public ApplyMessage(RemovedFromConfig removedFromConfig) : base(removedFromConfig) {}

    public bool IsExit => IsT0;
}
