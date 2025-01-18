using Google.Protobuf;

namespace RaftNET;

public partial class FSM {
    public class Config(bool enablePreVote, int appendRequestThreshold, int maxLogSize) : IDeepCloneable<Config> {
        // If set to true will enable pre-voting stage during election
        public bool EnablePreVote { get; set; } = enablePreVote;

        // max size of appended entries in bytes
        public int AppendRequestThreshold { get; set; } = appendRequestThreshold;

        // Limit in bytes on the size of the in-memory part of the log after which
        // requests are stopped until the log is shrunk by a is shrunk by a snapshot.
        // Should be greater than the sum of the following log entry sizes,
        // otherwise the state machine will deadlock.
        public int MaxLogSize { get; set; } = maxLogSize;

        public Config Clone() {
            return new Config(EnablePreVote, AppendRequestThreshold, MaxLogSize);
        }
    }
}
