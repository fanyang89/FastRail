namespace RaftNET.Exceptions;

public class NotLeaderException(ulong? currentLeader = null) : Exception {
    public override string Message => $"Not a leader, current leader: {currentLeader}";
}
