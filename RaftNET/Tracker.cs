using System.Collections;
using System.Diagnostics;
using Google.Protobuf.Collections;

namespace RaftNET;

public class Tracker {
    private Dictionary<ulong, FollowerProgress> _followers = new();
    private SortedSet<ulong> _currentVoters = new();
    private SortedSet<ulong> _previousVoters = new();

    public FollowerProgress? Find(ulong id) {
        return _followers.GetValueOrDefault(id);
    }

    private ISet<ulong> GetAllMembers(Configuration configuration) {
        var members = new SortedSet<ulong>();

        foreach (var member in configuration.Current) {
            if (member == null) {
                continue;
            }

            var id = member.ServerAddress.ServerId;
            members.Add(id);
        }

        foreach (var member in configuration.Previous) {
            if (member == null) {
                continue;
            }

            var id = member.ServerAddress.ServerId;
            members.Add(id);
        }

        return members;
    }

    private void SetConfigurationMembers(
        ISet<ulong> allMembers, // current and previous members
        ulong nextIdx,
        RepeatedField<ConfigMember> configMembers, // current or previous
        SortedSet<ulong> voters
    ) {
        foreach (var member in configMembers) {
            if (member == null) {
                continue;
            }

            var id = member.ServerAddress.ServerId;

            if (!allMembers.Contains(id)) {
                // this member is leaving cluster
                voters.Remove(id);
                _followers.Remove(id);
            } else {
                // this member is joining cluster or already joined
                if (member.CanVote) {
                    voters.Add(id);
                }

                if (!_followers.ContainsKey(id)) {
                    // create new follower progress for new member
                    _followers.Add(id, new FollowerProgress {
                        Id = id,
                        NextIdx = nextIdx,
                    });
                }
            }
        }
    }

    public void SetConfiguration(Configuration configuration, ulong nextIdx) {
        var allMembers = GetAllMembers(configuration);
        SetConfigurationMembers(allMembers, nextIdx, configuration.Current, _currentVoters);

        if (configuration.Previous != null) {
            SetConfigurationMembers(allMembers, nextIdx, configuration.Previous, _previousVoters);
        }
    }


    // Calculate the current commit index based on the current simple or joint quorum
    public ulong Committed(ulong prevCommitIdx) {
        var current = new MatchVector<ulong>(prevCommitIdx, _currentVoters.Count);

        if (_previousVoters.Count > 0) {
            var previous = new MatchVector<ulong>(prevCommitIdx, _previousVoters.Count);

            foreach (var (id, progress) in _followers) {
                if (_currentVoters.Contains(id)) {
                    current.Add(progress.MatchIdx);
                }

                if (_currentVoters.Contains(id)) {
                    previous.Add(progress.MatchIdx);
                }
            }

            if (!current.Committed() || !previous.Committed()) {
                return prevCommitIdx;
            }

            return ulong.Min(current.CommitIdx(), previous.CommitIdx());
        }

        foreach (var progress in from follower in _followers
                 let id = follower.Key
                 let progress = follower.Value
                 where _currentVoters.Contains(id)
                 select progress) {
            current.Add(progress.MatchIdx);
        }

        return !current.Committed() ? prevCommitIdx : current.CommitIdx();
    }
}