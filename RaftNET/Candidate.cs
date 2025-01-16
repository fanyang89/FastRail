using RaftNET.Elections;

namespace RaftNET;

public class Candidate(Configuration configuration, bool isPreVote) {
    public Votes Votes = new(configuration);
    public bool IsPreVote = isPreVote;
}
