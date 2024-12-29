namespace RaftNET;

public partial class FSM {
    public record Config(
        // If set to true will enable prevoting stage during election
        bool EnablePreVote,
        // max size of appended entries in bytes
        int AppendRequestThreshold,
        // Limit in bytes on the size of the in-memory part of the log after which
        // which requests are stopped until the log is shrunk by a is shrunk by a snapshot.
        // Should be greater than than the sum of the following log entry sizes,
        // otherwise the state machine will deadlock.
        int MaxLogSize
    );
}