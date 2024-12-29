namespace RaftNET;

public partial class FSM {
    public record Config(
        // If set to true will enable prevoting stage during election
        bool EnablePreVote,
        // max size of appended entries in bytes
        int AppendRequestThreshold,
        // Limit in bytes on the size of in-memory part of the log after
        // which requests are stopped to be admitted until the log
        // is shrunk back by a snapshot. Should be greater than
        // the sum of sizes of trailing log entries, otherwise the state
        // machine will deadlock.
        int MaxLogSize
    );
}