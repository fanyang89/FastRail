namespace RaftNET;

public partial class Fsm {
    public sealed record LastObservedState : IEquatable<Fsm> {
        public ulong CurrentTerm;
        public ulong VotedFor;
        public ulong CommitIdx;
        public ulong LastConfIdx;
        public ulong LastTerm;
        public bool AbortLeadershipTransfer;

        public bool Equals(Fsm? other) {
            if (other is null) {
                return false;
            }

            return CurrentTerm == other._currentTerm &&
                   VotedFor == other._votedFor &&
                   CommitIdx == other._commitIdx &&
                   LastConfIdx == other._log.LastConfIdx() &&
                   LastTerm == other._log.LastTerm() &&
                   AbortLeadershipTransfer == other._abortLeadershipTransfer;
        }

        public void Advance(Fsm fsm) {
            CurrentTerm = fsm._currentTerm;
            VotedFor = fsm._votedFor;
            CommitIdx = fsm._commitIdx;
            LastConfIdx = fsm._log.LastConfIdx();
            LastTerm = fsm._log.LastTerm();
            AbortLeadershipTransfer = fsm._abortLeadershipTransfer;
        }
    }
}