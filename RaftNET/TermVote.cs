namespace RaftNET;

public record TermVote(ulong Term, ulong VotedFor);