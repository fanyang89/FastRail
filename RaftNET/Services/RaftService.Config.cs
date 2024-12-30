using System.Net;
using Microsoft.Extensions.Logging;
using RaftNET.StateMachines;

namespace RaftNET.Services;

public partial class RaftService {
    public record Config(
        ulong MyId,
        string DataDir,
        ILoggerFactory LoggerFactory,
        IStateMachine StateMachine,
        AddressBook AddressBook,
        IPAddress ListenAddress,
        int Port,
        int PingInterval = 1000, // ms
        int PingTimeout = 1000, // ms
        int AppendRequestThreshold = 100000,
        bool EnablePreVote = true,
        // Limit in bytes on the size of in-memory part of the log after
        // which requests are stopped to be admitted until the log
        // is shrunk back by a snapshot.
        // The following condition must be satisfied:
        // max_command_size <= max_log_size - snapshot_trailing_size
        // this ensures that trailing log entries won't block incoming commands and at least
        // one command can fit in the log
        int MaxLogSize = 4 * 1024 * 1024,
        // Max size of a single command, add_entry with a bigger command will throw command_is_too_big_error.
        // The following condition must be satisfied:
        // max_command_size <= max_log_size - snapshot_trailing_size
        // this ensures that trailing log entries won't block incoming commands and at least
        // one command can fit in the log
        int MaxCommandSize = 100 * 1024
    );
}