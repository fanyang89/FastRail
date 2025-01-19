using OneOf;

namespace RaftNET.Tests.Replications;

public class UpdateFault : OneOfBase<Partition, Isolate, Disconnect, Stop> {
    protected UpdateFault(OneOf<Partition, Isolate, Disconnect, Stop> input) : base(input) {}
}