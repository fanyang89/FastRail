namespace RaftNET;

public class Leader {
    public Tracker Tracker;
    public long? stepDown;
    public ulong? TimeoutNowSent;

    private FSM FSM;
}