namespace RaftNET;

public class Candidate {
    public Votes Votes;
    public bool IsPreVote;

    public Candidate(Configuration configuration, bool isPreVote) {
        Votes = new Votes(configuration);
        IsPreVote = isPreVote;
    }
}
