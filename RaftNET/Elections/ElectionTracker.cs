using System.Diagnostics;
using Google.Protobuf.Collections;

namespace RaftNET.Elections;

public class ElectionTracker {
    private readonly ISet<ulong> _responded = new HashSet<ulong>();
    private readonly ISet<ulong> _votingMembers = new HashSet<ulong>();
    private int _granted;

    public ElectionTracker(RepeatedField<ConfigMember> members) {
        foreach (var member in members) {
            if (member.CanVote) {
                _votingMembers.Add(member.ServerAddress.ServerId);
            }
        }
    }

    public bool RegisterVote(ulong from, bool granted) {
        if (!_votingMembers.Contains(from)) {
            return false;
        }

        if (_responded.Add(from)) {
            if (granted) {
                _granted++;
            }
        }

        return true;
    }

    public VoteResult CountVotes() {
        var quorum = _votingMembers.Count / 2 + 1;

        if (_granted >= quorum) {
            return VoteResult.Won;
        }

        Debug.Assert(_responded.Count <= _votingMembers.Count);

        var unknown = _votingMembers.Count - _responded.Count;

        if (_granted + unknown >= quorum) {
            return VoteResult.Unknown;
        }

        return VoteResult.Lost;
    }
}