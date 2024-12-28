using OneOf;

namespace RaftNET;

public class Message : OneOfBase<VoteRequest, VoteResponse, AppendRequest, AppendResponse, InstallSnapshot, SnapshotResponse,
    TimeoutNowRequest> {
    public Message(
        OneOf<VoteRequest, VoteResponse, AppendRequest, AppendResponse, InstallSnapshot, SnapshotResponse, TimeoutNowRequest>
            input
    ) : base(input) {
    }

    public Message(VoteRequest request) : base(request) {}
    public Message(VoteResponse request) : base(request) {}
    public Message(AppendRequest request) : base(request) {}
    public Message(AppendResponse request) : base(request) {}
    public Message(InstallSnapshot request) : base(request) {}
    public Message(SnapshotResponse request) : base(request) {}
    public Message(TimeoutNowRequest request) : base(request) {}

    public ulong CurrentTerm() {
        return Match(
            x => x.CurrentTerm,
            x => x.CurrentTerm,
            x => x.CurrentTerm,
            x => x.CurrentTerm,
            x => x.CurrentTerm,
            x => x.CurrentTerm,
            x => x.CurrentTerm
        );
    }

    public VoteRequest VoteRequest() {
        return AsT0;
    }

    public VoteResponse VoteResponse() {
        return AsT1;
    }

    public AppendRequest AppendRequest() {
        return AsT2;
    }

    public AppendResponse AppendResponse() {
        return AsT3;
    }

    public InstallSnapshot InstallSnapshot() {
        return AsT4;
    }

    public SnapshotResponse SnapshotResponse() {
        return AsT5;
    }

    public TimeoutNowRequest TimeoutNowRequest() {
        return AsT6;
    }

    public bool IsVoteRequest() {
        return IsT0;
    }

    public bool IsVoteResponse() {
        return IsT1;
    }

    public bool IsAppendRequest() {
        return IsT2;
    }

    public bool IsAppendResponse() {
        return IsT3;
    }

    public bool IsInstallSnapshot() {
        return IsT4;
    }

    public bool IsSnapshotResponse() {
        return IsT5;
    }

    public bool IsTimeoutNowRequest() {
        return IsT6;
    }
}