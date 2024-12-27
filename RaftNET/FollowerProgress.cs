namespace RaftNET;

public class FollowerProgress {
    // ID of this server
    public ulong Id;

    // Index of the next log entry to send to this server.
    // Invariant: next_idx > match_idx.
    public ulong NextIdx;

    // Index of the highest log entry known to be replicated to this server.
    // More specifically, this is the greatest `last_new_idx` received from this follower
    // in an `accepted` message. As long as the follower remains in our term we know
    // that its log must match with ours up to (and including) `match_idx`.
    public ulong MatchIdx = 0;

    // Index that we know to be committed by the follower.
    ulong _commitIdx = 0;

    // Highest read ID the follower replied to.
    public ulong MaxAckedRead = 0;

    // True if the follower is a voting one.
    bool _canVote = true;

    FollowerProgressState _state = FollowerProgressState.Probe;

    // True if a packet was sent already in probe mode.
    bool _probeSent = false;

    // Number of in-flight still unACKed append entries requests.
    ulong _inFlight = 0;

    const ulong MaxInFlight = 10;

    public bool IsStrayReject(AppendRejected rejected) {
        if (rejected.NonMatchingIdx <= MatchIdx) {
            return true;
        }

        if (rejected.LastIdx < MatchIdx) {
            return true;
        }

        switch (_state) {
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
        _state = FollowerProgressState.Probe;
        _probeSent = false;
    }

    public void BecomePipeline() {
        if (_state != FollowerProgressState.Pipeline) {
            _state = FollowerProgressState.Pipeline;
            _inFlight = 0;
        }
    }

    public void BecomeSnapshot(ulong snp_idx) {
        _state = FollowerProgressState.Snapshot;
        NextIdx = snp_idx + 1;
    }

    public bool CanSendTo() {
        return _state switch {
            FollowerProgressState.Probe => !_probeSent,
            FollowerProgressState.Pipeline =>
                // allow `max_in_flight` outstanding indexes
                // FIXME: make it smarter
                _inFlight < MaxInFlight,
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