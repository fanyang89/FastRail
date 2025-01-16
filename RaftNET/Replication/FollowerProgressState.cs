namespace RaftNET.Replication;

public enum FollowerProgressState {
    // In this state only one append entry is sent until matching index is found.
    Probe,

    // In this state multiple append entries are sent optimistically.
    Pipeline,

    // In this state snapshot has been transferred.
    Snapshot
}
