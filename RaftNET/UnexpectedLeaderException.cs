namespace RaftNET;

public class UnexpectedLeaderException(ulong from, ulong currentLeader) : Exception {
    public override string Message =>
        $"Got append request/install snapshot/read_quorum from an unexpected leader {from}, expected {currentLeader}";
}