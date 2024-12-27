namespace RaftNET;

public class NotLeaderException : Exception {
    public override string Message => "Not a leader";
}