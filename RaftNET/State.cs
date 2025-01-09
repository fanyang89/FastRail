using System.Diagnostics;
using OneOf;

namespace RaftNET;

public class State : OneOfBase<Follower, Candidate, Leader> {
    protected State(OneOf<Follower, Candidate, Leader> input) : base(input) {}

    public State(Follower follower) : base(follower) {}
    public State(Candidate candidate) : base(candidate) {}
    public State(Leader leader) : base(leader) {}

    public bool IsLeader => IsT2;

    public Leader Leader {
        get {
            Debug.Assert(IsLeader);
            return AsT2;
        }
    }
}