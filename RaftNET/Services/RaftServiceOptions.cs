using Serilog;

namespace RaftNET.Services;

public record RaftServiceOptions {
    // max size of appended entries in bytes
    public int AppendRequestThreshold = 100000;

    // If set to true, forward configuration and entries from followers to the leader automatically.
    // This guarantees add_entry()/modify_config() never throws not_a_leader, but makes timed_out_error more likely.
    public bool EnableForwarding = true;

    // If set to true will enable pre-voting stage during election
    public bool EnablePreVote = true;

    // Max size of a single command, add_entry with a bigger command will throw command_is_too_big_error.
    // The following condition must be satisfied:
    // max_command_size <= max_log_size - snapshot_trailing_size
    // this ensures that trailing log entries won't block incoming commands and at least
    // one command can fit in the log
    public int MaxCommandSize = 100 * 1024;

    // Limit in bytes on the size of in-memory part of the log after
    // which requests are stopped to be admitted until the log
    // is shrunk back by a snapshot.
    // The following condition must be satisfied:
    // max_command_size <= max_log_size - snapshot_trailing_size
    // this ensures that trailing log entries won't block incoming commands and at least
    // one command can fit in the log
    public int MaxLogSize = 4 * 1024 * 1024;

    // A callback to invoke if one of internal server
    // background activities has stopped because of an error.
    public Action<Exception> OnBackgroundError = OnBackgroundErrorDefault;

    public int PingInterval = 1000; // ms

    public int PingTimeout = 1000; // ms

    // automatically snapshot state machine after applying this number of entries
    public ulong SnapshotThreshold = 1024;

    // Automatically snapshot state machine if the log memory usage exceeds this value.
    // The value is in bytes.
    // Must be smaller than max_log_size.
    // It is recommended to set this value to no more than half of the max_log_size,
    // so that snapshots are taken in advance, and there is no backpressure due to max_log_size.
    public ulong SnapshotThresholdLogSize = 2 * 1024 * 1024;

    // how many entries to leave in the log after taking a snapshot
    public ulong SnapshotTrailing = 200;

    // Limit on the total number of bytes, consumed by snapshot trailing entries.
    // Must be smaller than snapshot_threshold_log_size.
    // It is recommended to set this value to no more than half of snapshot_threshold_log_size
    // so that not all memory is held for trailing when taking a snapshot.
    public ulong SnapshotTrailingSize = 1 * 1024 * 1024;

    public static void OnBackgroundErrorDefault(Exception ex) {
        Log.Error(ex, "Unexpected background error");
    }
}
