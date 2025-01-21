namespace RaftNET;

public partial class FSM {
    public sealed record LastObservedState : IEquatable<FSM> {
        public ulong CurrentTerm;
        public ulong VotedFor;
        public ulong CommitIdx;
        public ulong LastConfIdx;
        public ulong LastTerm;
        public bool AbortLeadershipTransfer;

        public bool Equals(FSM? other) {
            if (other is null) {
                return false;
            }

            return CurrentTerm == other.CurrentTerm &&
                   VotedFor == other._votedFor &&
                   CommitIdx == other._commitIdx &&
                   LastConfIdx == other.RaftLog.LastConfIdx &&
                   LastTerm == other.RaftLog.LastTerm() &&
                   AbortLeadershipTransfer == other._abortLeadershipTransfer;
        }

        public void Advance(FSM fsm) {
            CurrentTerm = fsm.CurrentTerm;
            VotedFor = fsm._votedFor;
            CommitIdx = fsm._commitIdx;
            LastConfIdx = fsm.RaftLog.LastConfIdx;
            LastTerm = fsm.RaftLog.LastTerm();
            AbortLeadershipTransfer = fsm._abortLeadershipTransfer;
        }
    }
}
