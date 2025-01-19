using OneOf;

namespace RaftNET.Tests.ReplicationTests;

public class Partition : OneOfBase<Leader, Range, int> {
    protected Partition(OneOf<Leader, Range, int> input) : base(input) {}
}
