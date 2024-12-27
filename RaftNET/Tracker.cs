namespace RaftNET;

public class Tracker {
    private readonly SortedSet<ulong> _currentVoters = new();
    private Dictionary<ulong, FollowerProgress> _followerProgressList = new();
    private readonly SortedSet<ulong> _previousVoters = new();

    public SortedSet<ulong> CurrentVoters => _currentVoters;
    public SortedSet<ulong> PreviousVoters => _previousVoters;
    public Dictionary<ulong, FollowerProgress> FollowerProgresses => _followerProgressList;

    public FollowerProgress? Find(ulong id) {
        return _followerProgressList.GetValueOrDefault(id);
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

    public void SetConfiguration(Configuration configuration, ulong nextIdx) {
        _currentVoters.Clear();
        _previousVoters.Clear();

        var oldProgress = _followerProgressList;
        _followerProgressList = new Dictionary<ulong, FollowerProgress>();

        foreach (var member in configuration.Current) {
            var id = member.ServerAddress.ServerId;

            if (member.CanVote) {
                _currentVoters.Add(id);
            }

            if (_followerProgressList.ContainsKey(id)) {
                continue;
            }

            if (oldProgress.TryGetValue(id, out var value)) {
                _followerProgressList[id] = value;
                _followerProgressList[id].CanVote = member.CanVote;
            } else {
                _followerProgressList.Add(id, new FollowerProgress {
                    Id = id,
                    NextIdx = nextIdx,
                    CanVote = member.CanVote
                });
            }
        }

        if (configuration.Previous.Count > 0) {
            foreach (var member in configuration.Previous) {
                var id = member.ServerAddress.ServerId;

                if (member.CanVote) {
                    _previousVoters.Add(id);
                }

                if (_followerProgressList.ContainsKey(id)) {
                    continue;
                }

                if (oldProgress.TryGetValue(id, out var value)) {
                    _followerProgressList[id] = value;
                    _followerProgressList[id].CanVote = member.CanVote;
                } else {
                    _followerProgressList.Add(id, new FollowerProgress {
                        Id = id,
                        NextIdx = nextIdx,
                        CanVote = member.CanVote
                    });
                }
            }
        }
    }


    // Calculate the current commit index based on the current simple or joint quorum
    public ulong Committed(ulong prevCommitIdx) {
        var current = new MatchVector<ulong>(prevCommitIdx, _currentVoters.Count);

        if (_previousVoters.Count > 0) {
            var previous = new MatchVector<ulong>(prevCommitIdx, _previousVoters.Count);

            foreach (var (id, progress) in _followerProgressList) {
                if (_currentVoters.Contains(id)) {
                    current.Add(progress.MatchIdx);
                }

                if (_previousVoters.Contains(id)) {
                    previous.Add(progress.MatchIdx);
                }
            }

            if (!current.Committed() || !previous.Committed()) {
                return prevCommitIdx;
            }

            return ulong.Min(current.CommitIdx(), previous.CommitIdx());
        }

        foreach (var (id, progress) in _followerProgressList)
            if (_currentVoters.Contains(id)) {
                current.Add(progress.MatchIdx);
            }

        if (!current.Committed()) {
            return prevCommitIdx;
        }

        return current.CommitIdx();
    }

    public ActivityTracker GetActivityTracker() {
        return new ActivityTracker(this);
    }
}