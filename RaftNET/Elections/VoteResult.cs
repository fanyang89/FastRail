namespace RaftNET.Elections;

// Possible leader election outcomes.
public enum VoteResult {
    // We haven't got enough responses yet, either because
    // the servers haven't voted or responses failed to arrive.
    Unknown = 0,

    // This candidate has won the election
    Won,

    // The quorum of servers has voted against this candidate
    Lost
}
