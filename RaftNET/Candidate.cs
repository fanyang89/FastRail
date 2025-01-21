using RaftNET.Elections;

namespace RaftNET;

public class Candidate(Configuration configuration, bool isPreVote) {
    public bool IsPreVote = isPreVote;
    public Votes Votes = new(configuration);
}
