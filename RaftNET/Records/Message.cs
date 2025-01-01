using OneOf;

namespace RaftNET.Records;

public class Message : OneOfBase<VoteRequest, VoteResponse, AppendRequest, AppendResponse, InstallSnapshot, SnapshotResponse,
    TimeoutNowRequest> {
    public Message(
        OneOf<VoteRequest, VoteResponse, AppendRequest, AppendResponse, InstallSnapshot, SnapshotResponse, TimeoutNowRequest>
            input
    ) : base(input) {}

    public Message(VoteRequest request) : base(request) {}
    public Message(VoteResponse request) : base(request) {}
    public Message(AppendRequest request) : base(request) {}
    public Message(AppendResponse request) : base(request) {}
    public Message(InstallSnapshot request) : base(request) {}
    public Message(SnapshotResponse request) : base(request) {}
    public Message(TimeoutNowRequest request) : base(request) {}

    public ulong CurrentTerm => Match(
        x => x.CurrentTerm,
        x => x.CurrentTerm,
        x => x.CurrentTerm,
        x => x.CurrentTerm,
        x => x.CurrentTerm,
        x => x.CurrentTerm,
        x => x.CurrentTerm
    );

    public VoteRequest VoteRequest => AsT0;
    public VoteResponse VoteResponse => AsT1;
    public AppendRequest AppendRequest => AsT2;
    public AppendResponse AppendResponse => AsT3;
    public InstallSnapshot InstallSnapshot => AsT4;
    public SnapshotResponse SnapshotResponse => AsT5;
    public TimeoutNowRequest TimeoutNowRequest => AsT6;
    public bool IsVoteRequest => IsT0;
    public bool IsVoteResponse => IsT1;
    public bool IsAppendRequest => IsT2;
    public bool IsAppendResponse => IsT3;
    public bool IsInstallSnapshot => IsT4;
    public bool IsSnapshotResponse => IsT5;
    public bool IsTimeoutNowRequest => IsT6;
}