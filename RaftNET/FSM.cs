using System.Diagnostics;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using OneOf;

namespace RaftNET;

using Message = OneOf<
    VoteRequest, VoteResponse, AppendRequest, AppendResponse,
    InstallSnapshot, SnapshotResponse, TimeoutNowRequest>;

public partial class FSM {
    private ulong _myID;
    private OneOf<Follower, Candidate, Leader> _state;
    private ulong _currentTerm;
    private ulong _votedFor;
    private ulong _commitIdx;
    private Log _log;
    private FSMConfig _config;
    private bool _abortLeadershipTransfer = false;
    private bool _pingLeader = false;
    private FSMOutput _output;
    private LogicalClock _clock;
    private long _lastElectionTime = 0;
    private long _randomizedElectionTimeout = ElectionTimeout + 1;
    private IFailureDetector _failureDetector;
    private readonly ThreadLocal<Random> _random = new(() => new Random());
    private List<KeyValuePair<ulong, Message>> _messages = new();
    private readonly ILogger<FSM> _logger;
    private readonly LastObservedState _observed = new LastObservedState();

    public const long ElectionTimeout = 10;

    public FSM(ILogger<FSM>? logger = null) {
        _logger = logger ?? new NullLogger<FSM>();
        _observed.Advance(this);
    }

    public bool IsFollower => _state.IsT0;
    public bool IsCandidate => _state.IsT1;
    public bool IsLeader => _state.IsT2;
    public string CurrentState => IsLeader ? "Leader" : IsFollower ? "Follower" : "Candidate";
    public bool IsPreVoteCandidate => IsCandidate && _state.AsT1.IsPreVote;
    public ulong CurrentLeader => IsLeader ? _myID : IsFollower ? _state.AsT0.CurrentLeader : 0;
    public ulong CurrentTerm => _currentTerm;
    public long ElectionElapsed => _clock.Now() - _lastElectionTime;

    private Follower FollowerState => _state.AsT0;
    private Candidate CandidateState => _state.AsT1;
    private Leader LeaderState => _state.AsT2;

    public bool HasOutput() {
        var diff = _log.LastIdx() - _log.StableIdx();
        return diff > 0 || _messages.Any() || _output.StateChanged || !_observed.Equals(this);
    }

    void Replicate() {
        Debug.Assert(IsLeader);

        foreach (var (id, progress) in LeaderState.Tracker.FollowerProgresses) {
            if (progress.Id != _myID) {
                ReplicateTo(progress, false);
            }
        }
    }

    public FSMOutput GetOutput() {
        var diff = _log.LastIdx() - _log.StableIdx();

        if (IsLeader) {
            if (diff > 0) {
                Replicate();
            }
        }

        var output = _output;
        _output = new FSMOutput();

        if (diff > 0) {
            for (var i = _log.StableIdx() + 1; i < _log.LastIdx(); ++i) {
                output.LogEntries.Add(_log[i]);
            }
        }

        if (_observed.CurrentTerm != _currentTerm || _observed.VotedFor != _votedFor) {
            output.TermAndVote = new(_currentTerm, _votedFor);
        }

        var observed_ci = ulong.Max(_observed.CommitIdx, _log.GetSnapshot().Idx);

        if (observed_ci < _commitIdx) {
            for (var idx = observed_ci + 1; idx <= _commitIdx; ++idx) {
                var entry = _log[idx];
                output.Committed.Add(entry);
            }
        }

        output.Messages = _messages;
        _messages = new List<KeyValuePair<ulong, Message>>();

        output.AbortLeadershipTransfer = _abortLeadershipTransfer;
        _abortLeadershipTransfer = false;

        if (_observed.LastConfIdx != _log.LastConfIdx() ||
            (_observed.CurrentTerm != _log.LastTerm() &&
             _observed.LastTerm != _log.LastTerm())) {
            output.Configuration = new HashSet<ConfigMember>();
            var lastLogConf = _log.GetConfiguration();
            foreach (var member in lastLogConf.Previous) {
                output.Configuration.Add(member);
            }
            foreach (var member in lastLogConf.Current) {
                output.Configuration.Add(member);
            }
        }

        _observed.Advance(this);

        if (output.LogEntries.Count > 0) {
        }

        return output;
    }

    void AdvanceStableIdx(ulong idx) {
        var prevStableIdx = _log.StableIdx();
        _log.StableTo(idx);
        if (IsLeader) {
            var leaderProgress = LeaderState.Tracker.Find(_myID);
            if (leaderProgress != null) {
                // If this server is leader and is part of the current
                // configuration, update its progress and optionally
                // commit new entries.
                leaderProgress.Accepted(idx);
                MaybeCommit();
            }
        }
    }

    private void MaybeCommit() {
        var newCommitIdx = LeaderState.Tracker.Committed(_commitIdx);

        if (newCommitIdx <= _commitIdx) {
            return;
        }

        var committedConfChange = _commitIdx < _log.LastConfIdx() && newCommitIdx >= _log.LastConfIdx();
        if (_log[newCommitIdx].Term != _currentTerm) {
            return;
        }

        _commitIdx = newCommitIdx;

        if (!committedConfChange) {
            return;
        }

        if (_log.GetConfiguration().IsJoint()) {
            var cfg = _log.GetConfiguration();
            cfg.LeaveJoint();
            _log.Add(new LogEntry {
                Term = _currentTerm,
                Idx = _log.NextIdx(),
                Configuration = cfg,
            });
            LeaderState.Tracker.SetConfiguration(_log.GetConfiguration(), _log.LastIdx());
            MaybeCommit();
        } else {
            var lp = LeaderState.Tracker.Find(_myID);
            if (lp is not { CanVote: true }) {
                _logger.LogTrace("Stepping down as leader, my_id={}", _myID);
                TransferLeadership();
            }
        }
    }

    private bool HasStableLeader() {
        var cfg = _log.GetConfiguration();
        var currentLeader = CurrentLeader;
        return currentLeader > 0 && cfg.CanVote(currentLeader) && _failureDetector.IsAlive(currentLeader);
    }

    public void Tick() {
        _clock.Advance();

        if (IsLeader) {
            TickLeader();
        } else if (HasStableLeader()) {
            _lastElectionTime = _clock.Now();
        } else if (ElectionElapsed >= _randomizedElectionTimeout) {
            BecomeCandidate(_config.EnablePreVote);
        }

        if (IsFollower && CurrentLeader == 0 && _pingLeader) {
            // TODO: ping leader
        }
    }

    private void BecomeCandidate(bool isPreVote, bool isLeadershipTransfer = false) {
        if (!IsCandidate) {
            _output.StateChanged = true;
        }

        _state = new Candidate(_log.GetConfiguration(), isPreVote);
        ResetElectionTimeout();

        _lastElectionTime = _clock.Now();
        var votes = CandidateState.Votes;
        var voters = votes.Voters;

        if (voters.All(x => x.ServerId != _myID)) {
            if (_log.LastConfIdx() <= _commitIdx) {
                BecomeFollower(0);
                return;
            }

            var prevConfig = _log.GetPreviousConfiguration();
            Debug.Assert(prevConfig != null);

            if (!prevConfig.CanVote(_myID)) {
                BecomeFollower(0);
                return;
            }
        }

        ulong term = _currentTerm + 1;

        if (!isPreVote) {
            UpdateCurrentTerm(term);
        }

        foreach (var server in voters) {
            if (server.ServerId == _myID) {
                votes.RegisterVote(server.ServerId, true);

                if (!isPreVote) {
                    _votedFor = _myID;
                }

                continue;
            }

            SendTo(server.ServerId, new VoteRequest {
                CurrentTerm = term,
                Force = isLeadershipTransfer,
                IsPreVote = isPreVote,
                LastLogIdx = _log.LastIdx(),
                LastLogTerm = _log.LastTerm(),
            });
        }

        if (votes.CountVotes() == VoteResult.Won) {
            if (isPreVote) {
                BecomeCandidate(false);
            } else {
                BecomeLeader();
            }
        }
    }


    private void SendTo(ulong to, Message message) {
        _messages.Add(new KeyValuePair<ulong, Message>(to, message));
    }

    private void UpdateCurrentTerm(ulong term) {
        Debug.Assert(term > _currentTerm);
        _currentTerm = term;
        _votedFor = 0;
    }

    private void BecomeFollower(ulong leaderId) {
        throw new NotImplementedException();
    }

    private void BecomeLeader() {
        Debug.Assert(!IsLeader);
        _output.StateChanged = true;
        _state = new Leader();
        _lastElectionTime = _clock.Now();
        _pingLeader = false;
        AddEntry(new Dummy());
        LeaderState.Tracker.SetConfiguration(_log.GetConfiguration(), _log.LastIdx());
    }

    void CheckIsLeader() {
        if (!IsLeader) {
            throw new NotLeaderException();
        }
    }

    public LogEntry AddEntry(OneOf<Dummy, byte[], Configuration> command) {
        CheckIsLeader();

        if (LeaderState.stepDown != null) {
            throw new NotLeaderException();
        }

        var isConfig = command.IsT2;

        if (isConfig) {
            Messages.CheckConfiguration(command.AsT2.Current);

            if (_log.LastConfIdx() > _commitIdx || _log.GetConfiguration().IsJoint()) {
                throw new ConfigurationChangeInProgressException();
            }

            var tmp = _log.GetConfiguration().Clone();
            tmp.EnterJoint(command.AsT2.Current);
            command = tmp;
        }

        var logEntry = new LogEntry {
            Term = _currentTerm,
            Idx = _log.NextIdx(),
        };
        command.Switch(
            _ => { logEntry.Dummy = new Void(); },
            buffer => { logEntry.Command = new Command { Buffer = ByteString.CopyFrom(buffer) }; },
            config => { logEntry.Configuration = config; }
        );
        _log.Add(logEntry);

        if (isConfig) {
            LeaderState.Tracker.SetConfiguration(_log.GetConfiguration(), _log.LastIdx());
        }

        return _log[_log.LastIdx()];
    }

    private void ResetElectionTimeout() {
        Debug.Assert(_random.Value != null);
        var upper = long.Max(ElectionTimeout, _log.GetConfiguration().Current.Count);
        _randomizedElectionTimeout = ElectionTimeout + _random.Value.NextInt64(1, upper);
    }

    private void TickLeader() {
        if (ElectionElapsed >= ElectionTimeout) {
            BecomeFollower(0);
            return;
        }

        var state = LeaderState;
        var active = state.Tracker.GetActivityTracker();
        active.Record(_myID);

        foreach (var (id, progress) in state.Tracker.FollowerProgresses) {
            if (progress.Id != _myID) {
                if (_failureDetector.IsAlive(progress.Id)) {
                    active.Record(progress.Id);
                }

                switch (progress.State) {
                    case FollowerProgressState.Probe:
                        progress.ProbeSent = false;
                        break;
                    case FollowerProgressState.Pipeline:
                        if (progress.InFlight == FollowerProgress.MaxInFlight) {
                            progress.InFlight--;
                        }

                        break;
                    case FollowerProgressState.Snapshot:
                        continue;
                    default:
                        throw new ArgumentOutOfRangeException();
                }

                if (progress.MatchIdx < _log.LastIdx() || progress.CommitIdx < _commitIdx) {
                    ReplicateTo(progress, true);
                }
            }
        }

        if (active.Record()) {
            _lastElectionTime = _clock.Now();
        }

        if (state.stepDown != null) {
            var me = state.Tracker.Find(_myID);

            if (me == null || !me.CanVote) {
                _logger.LogTrace("not aborting step down: we have been removed from the configuration");
            } else if (state.stepDown <= _clock.Now()) {}

            throw new NotImplementedException();
        }
    }

    private void ReplicateTo(FollowerProgress progress, bool b) {
        throw new NotImplementedException();
    }

    public void Step(ulong from, Message msg) {
        Debug.Assert(from != _myID, "fsm cannot process messages from itself");
        if (msg.CurrentTerm() > _currentTerm) {
            ulong leader = 0;
            if (msg.IsAppendRequest() || msg.IsInstallSnapshot()) {
                leader = from;
            }

            bool ignoreTerm = false;
            if (msg.IsVoteRequest()) {
                ignoreTerm = msg.VoteRequest().IsPreVote;
            } else if (msg.IsVoteResponse()) {
                var rsp = msg.VoteResponse();
                ignoreTerm = rsp.IsPreVote && rsp.VoteGranted;
            }

            if (!ignoreTerm) {
                BecomeFollower(leader);
                UpdateCurrentTerm(msg.CurrentTerm());
            }
        } else if (msg.CurrentTerm() < _currentTerm) {
            if (msg.IsAppendRequest()) {
                SendTo(from, new AppendResponse {
                    CurrentTerm = _currentTerm,
                    CommitIdx = _commitIdx,
                    Rejected = new AppendRejected {
                        LastIdx = _log.LastIdx(),
                        NonMatchingIdx = 0
                    }
                });
            } else if (msg.IsInstallSnapshot()) {
                SendTo(from, new SnapshotResponse {
                    CurrentTerm = _currentTerm,
                    Success = false
                });
            } else if (msg.IsVoteRequest()) {
                if (msg.VoteRequest().IsPreVote) {
                    SendTo(from, new VoteResponse {
                        CurrentTerm = _currentTerm,
                        VoteGranted = false,
                        IsPreVote = true
                    });
                }
            } else {
                _logger.LogTrace("ignored a message with lower term from {}, term: {}", from, msg.CurrentTerm());
            }
            return;
        } else {
            // _current_term == msg.current_term
            if (msg.IsAppendRequest() || msg.IsInstallSnapshot()) {
                if (IsCandidate) {
                    BecomeFollower(from);
                } else if (CurrentLeader == 0) {
                    FollowerState.CurrentLeader = from;
                    _pingLeader = false;
                }
                _lastElectionTime = _clock.Now();
                if (CurrentLeader != from) {
                    throw new UnexpectedLeaderException(from, CurrentLeader);
                }
            }
        }

        _state.Switch(
            follower => { Step(from, follower, msg); },
            candidate => { Step(from, candidate, msg); },
            leader => { Step(from, leader, msg); }
        );
    }

    public void Step(ulong from, Leader leader, Message message) {
        message.Switch(
            voteRequest => {
                RequestVote(from, voteRequest);
            },
            voteResponse => {
                // ignored
            },
            appendRequest => {
                // ignored
            },
            appendResponse => {
                AppendEntriesResponse(from, appendResponse);
            },
            installSnapshot => {
                SendTo(from, new SnapshotResponse {
                    CurrentTerm = _currentTerm,
                    Success = false
                });
            },
            snapshotResponse => {
                InstallSnapshotResponse(from, snapshotResponse);
            },
            timeoutNowRequest => {
                // ignored
            }
        );
    }

    private void InstallSnapshotResponse(ulong from, SnapshotResponse snapshotResponse) {
        throw new NotImplementedException();
    }

    private void RequestVote(ulong from, VoteRequest voteRequest) {
        throw new NotImplementedException();
    }

    private void AppendEntriesResponse(ulong from, AppendResponse appendResponse) {
        throw new NotImplementedException();
    }

    public void Step(ulong from, Candidate candidate, Message message) {
        message.Switch(
            voteRequest => {
                RequestVote(from, voteRequest);
            },
            voteResponse => {
                RequestVoteResponse(from, voteResponse);
            },
            appendRequest => {
                // ignored
            },
            appendResponse => {
                // ignored
            },
            installSnapshot => {
                SendTo(from, new SnapshotResponse {
                    CurrentTerm = _currentTerm,
                    Success = false
                });
            },
            snapshotResponse => {
                // ignored
            },
            timeoutNowRequest => {
                // ignored
            }
        );
    }

    private void RequestVoteResponse(ulong from, VoteResponse voteResponse) {
        throw new NotImplementedException();
    }

    public void Step(ulong from, Follower follower, Message message) {
        message.Switch(
            voteRequest => {
                RequestVote(from, voteRequest);
            },
            voteResponse => {
                // ignored
            },
            appendRequest => {
                AppendEntries(from, appendRequest);
            },
            appendResponse => {
                // ignored
            },
            installSnapshot => {
                bool success = ApplySnapshot(installSnapshot.Snp, 0, 0, false);
                SendTo(from, new SnapshotResponse {
                    CurrentTerm = _currentTerm,
                    Success = success
                });
            },
            snapshotResponse => {
                // ignored
            },
            timeoutNowRequest => {
                BecomeCandidate(false, true);
            }
        );
    }

    private void AppendEntries(ulong from, AppendRequest appendRequest) {
        throw new NotImplementedException();
    }

    public void TransferLeadership(long timeout = 0) {}

    public bool ApplySnapshot(SnapshotDescriptor snapshot, int maxTrailingEntries, int maxTrailingBytes, bool local) {
        throw new NotImplementedException();
    }
}
