﻿using OneOf;

namespace RaftNET.Tests.Replications;

public class Partition : OneOfBase<Leader, Range, int> {
    protected Partition(OneOf<Leader, Range, int> input) : base(input) {}
}