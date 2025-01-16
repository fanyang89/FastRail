namespace RaftNET.Replication;

public class FollowerProgress {
    public const ulong MaxInFlight = 10;

    // True if the follower is a voting one.
    public bool CanVote = true;

    // ID of this server
    public ulong Id;

    // Index of the highest log entry known to be replicated to this server.
    // More specifically, this is the greatest `last_new_idx` received from this follower
    // in an `accepted` message. As long as the follower remains in our term we know
    // that its log must match with ours up to (and including) `match_idx`.
    public ulong MatchIdx;

    // Highest read ID the follower replied to.
    public ulong MaxAckedRead = 0;

    // Index of the next log entry to send to this server.
    // Invariant: next_idx > match_idx.
    public ulong NextIdx;

    // True if a packet was sent already in probe mode.

    // Index that we know to be committed by the follower.
    public ulong CommitIdx { get; set; }

    // Number of in-flight still unACKed append entries requests.
    public ulong InFlight { get; set; }

    public FollowerProgressState State { get; private set; } = FollowerProgressState.Probe;

    public bool ProbeSent { get; set; }

    public bool IsStrayReject(AppendRejected rejected) {
        if (rejected.NonMatchingIdx <= MatchIdx) {
            return true;
        }

        if (rejected.LastIdx < MatchIdx) {
            return true;
        }

        switch (State) {
            case FollowerProgressState.Pipeline:
                break;
            case FollowerProgressState.Probe:
                if (rejected.NonMatchingIdx != NextIdx - 1) {
                    return true;
                }

                break;
            case FollowerProgressState.Snapshot:
                return true;
            default:
                throw new ArgumentOutOfRangeException();
        }

        return false;
    }

    public void BecomeProbe() {
        State = FollowerProgressState.Probe;
        ProbeSent = false;
    }

    public void BecomePipeline() {
        if (State != FollowerProgressState.Pipeline) {
            State = FollowerProgressState.Pipeline;
            InFlight = 0;
        }
    }

    public void BecomeSnapshot(ulong snp_idx) {
        State = FollowerProgressState.Snapshot;
        NextIdx = snp_idx + 1;
    }

    public bool CanSendTo() {
        return State switch {
            FollowerProgressState.Probe => !ProbeSent,
            FollowerProgressState.Pipeline =>
                // allow `max_in_flight` outstanding indexes
                // FIXME: make it smarter
                InFlight < MaxInFlight,
            FollowerProgressState.Snapshot =>
                // In this state we are waiting
                // for a snapshot to be transferred
                // before starting to sync the log.
                false,
            _ => false
        };
    }

    public void Accepted(ulong idx) {
        // AppendEntries replies can arrive out of order.
        MatchIdx = ulong.Max(idx, MatchIdx);
        // idx may be smaller if we increased next_idx optimistically in pipeline mode.
        NextIdx = ulong.Max(idx + 1, NextIdx);
    }
}
