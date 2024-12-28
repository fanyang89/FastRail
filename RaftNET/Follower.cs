namespace RaftNET;

public class Follower(ulong leader) {
    public ulong CurrentLeader { get; set; } = leader;
}