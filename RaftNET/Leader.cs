namespace RaftNET;

public class Leader {
    public Tracker Tracker { get; set; } = new();
    public long? stepDown;
    public ulong? TimeoutNowSent;

    private FSM FSM;
}