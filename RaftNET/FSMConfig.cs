namespace RaftNET;

public class FSMConfig {
    public bool EnablePreVote { get; set; }

    // max size of appended entries in bytes
    public int AppendRequestThreshold { get; set; }
}