namespace RaftNET;

public class Leader {
    public Tracker Tracker;
    private FSM FSM;
    public long? stepDown;
}
