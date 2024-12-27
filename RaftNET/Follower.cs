namespace RaftNET;

public class Follower(ulong leader) {
    public ulong CurrentLeader = leader;
}