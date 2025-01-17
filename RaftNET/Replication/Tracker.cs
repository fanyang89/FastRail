namespace RaftNET.Replication;

public class Tracker {
    public SortedSet<ulong> CurrentVoters { get; } = new();

    public SortedSet<ulong> PreviousVoters { get; } = new();

    public Dictionary<ulong, FollowerProgress> FollowerProgresses { get; private set; } = new();

    public FollowerProgress? Find(ulong id) {
        return FollowerProgresses.GetValueOrDefault(id);
    }

    public void SetConfiguration(Configuration configuration, ulong nextIdx) {
        CurrentVoters.Clear();
        PreviousVoters.Clear();

        var oldProgress = FollowerProgresses;
        FollowerProgresses = new Dictionary<ulong, FollowerProgress>();

        foreach (var member in configuration.Current) {
            var id = member.ServerAddress.ServerId;

            if (member.CanVote) {
                CurrentVoters.Add(id);
            }

            if (FollowerProgresses.ContainsKey(id)) {
                continue;
            }

            if (oldProgress.TryGetValue(id, out var value)) {
                FollowerProgresses[id] = value;
                FollowerProgresses[id].CanVote = member.CanVote;
            } else {
                FollowerProgresses.Add(id, new FollowerProgress { Id = id, NextIdx = nextIdx, CanVote = member.CanVote });
            }
        }

        if (configuration.Previous.Count > 0) {
            foreach (var member in configuration.Previous) {
                var id = member.ServerAddress.ServerId;

                if (member.CanVote) {
                    PreviousVoters.Add(id);
                }

                if (FollowerProgresses.ContainsKey(id)) {
                    continue;
                }

                if (oldProgress.TryGetValue(id, out var value)) {
                    FollowerProgresses[id] = value;
                    FollowerProgresses[id].CanVote = member.CanVote;
                } else {
                    FollowerProgresses.Add(id, new FollowerProgress { Id = id, NextIdx = nextIdx, CanVote = member.CanVote });
                }
            }
        }
    }

    // Calculate the current commit index based on the current simple or joint quorum
    public ulong Committed(ulong prevCommitIdx) {
        var current = new MatchVector<ulong>(prevCommitIdx, CurrentVoters.Count);

        if (PreviousVoters.Count > 0) {
            var previous = new MatchVector<ulong>(prevCommitIdx, PreviousVoters.Count);
            foreach (var (id, progress) in FollowerProgresses) {
                if (CurrentVoters.Contains(id)) {
                    current.Add(progress.MatchIdx);
                }
                if (PreviousVoters.Contains(id)) {
                    previous.Add(progress.MatchIdx);
                }
            }
            if (!current.Committed() || !previous.Committed()) {
                return prevCommitIdx;
            }
            return ulong.Min(current.CommitIdx(), previous.CommitIdx());
        }

        foreach (var (id, progress) in FollowerProgresses) {
            if (CurrentVoters.Contains(id)) {
                current.Add(progress.MatchIdx);
            }
        }
        return current.Committed() ? current.CommitIdx() : prevCommitIdx;
    }

    public ulong CommittedReadId(ulong prevCommitIdx) {
        var current = new MatchVector<ulong>(prevCommitIdx, CurrentVoters.Count);

        if (PreviousVoters.Count > 0) {
            var previous = new MatchVector<ulong>(prevCommitIdx, PreviousVoters.Count);
            foreach (var (id, progress) in FollowerProgresses) {
                if (CurrentVoters.Contains(id)) {
                    current.Add(progress.MaxAckedRead);
                }
                if (PreviousVoters.Contains(id)) {
                    previous.Add(progress.MaxAckedRead);
                }
            }
            if (!current.Committed() || !previous.Committed()) {
                return prevCommitIdx;
            }
            return ulong.Min(current.CommitIdx(), previous.CommitIdx());
        }

        foreach (var (id, progress) in FollowerProgresses) {
            if (CurrentVoters.Contains(id)) {
                current.Add(progress.MaxAckedRead);
            }
        }
        return current.Committed() ? current.CommitIdx() : prevCommitIdx;
    }

    public ActivityTracker GetActivityTracker() {
        return new ActivityTracker(this);
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
}
