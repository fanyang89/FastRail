using OneOf;

namespace RaftNET.Services;

public class ApplyMessage : OneOfBase<List<LogEntry>, SnapshotDescriptor, OnExit> {
    protected ApplyMessage(OneOf<List<LogEntry>, SnapshotDescriptor, OnExit> input) : base(input) {}

    public ApplyMessage(List<LogEntry> entries) : base(entries) {}
    public ApplyMessage(SnapshotDescriptor snapshotDescriptor) : base(snapshotDescriptor) {}

    public ApplyMessage() : base(new OnExit()) {}

    public bool IsExit => IsT2;
}
