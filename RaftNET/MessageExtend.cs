using OneOf;

namespace RaftNET;

using Message = OneOf<VoteRequest, VoteResponse, AppendRequest, AppendResponse, InstallSnapshot, SnapshotResponse,
    TimeoutNowRequest>;

static class MessageExtend {
    public static ulong CurrentTerm(
        this Message msg
    ) {
        return msg.Match(
            x => x.CurrentTerm,
            x => x.CurrentTerm,
            x => x.CurrentTerm,
            x => x.CurrentTerm,
            x => x.CurrentTerm,
            x => x.CurrentTerm,
            x => x.CurrentTerm
        );
    }

    public static VoteRequest VoteRequest(
        this Message msg
    ) {
        return msg.AsT0;
    }

    public static VoteResponse VoteResponse(
        this Message msg
    ) {
        return msg.AsT1;
    }

    public static AppendRequest AppendRequest(
        this Message msg
    ) {
        return msg.AsT2;
    }

    public static AppendResponse AppendResponse(
        this Message msg
    ) {
        return msg.AsT3;
    }

    public static InstallSnapshot InstallSnapshot(
        this Message msg
    ) {
        return msg.AsT4;
    }

    public static SnapshotResponse SnapshotResponse(
        this Message msg
    ) {
        return msg.AsT5;
    }

    public static TimeoutNowRequest TimeoutNowRequest(
        this Message msg
    ) {
        return msg.AsT6;
    }

    public static bool IsVoteRequest(
        this Message msg
    ) {
        return msg.IsT0;
    }

    public static bool IsVoteResponse(
        this Message msg
    ) {
        return msg.IsT1;
    }

    public static bool IsAppendRequest(
        this Message msg
    ) {
        return msg.IsT2;
    }

    public static bool IsAppendResponse(
        this Message msg
    ) {
        return msg.IsT3;
    }

    public static bool IsInstallSnapshot(
        this Message msg
    ) {
        return msg.IsT4;
    }

    public static bool IsSnapshotResponse(
        this Message msg
    ) {
        return msg.IsT5;
    }

    public static bool IsTimeoutNowRequest(
        this Message msg
    ) {
        return msg.IsT6;
    }
}