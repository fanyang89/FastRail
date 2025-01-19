using OneOf;

namespace RaftNET.Tests.ReplicationTests;

public class UpdateFault : OneOfBase<Partition, Isolate, Disconnect, Stop> {
    protected UpdateFault(OneOf<Partition, Isolate, Disconnect, Stop> input) : base(input) {}
}
