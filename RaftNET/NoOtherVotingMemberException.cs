namespace RaftNET;

public class NoOtherVotingMemberException : Exception {
    public override string Message { get; } = "No other voting member";
}