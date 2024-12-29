using System.Diagnostics;
using Google.Protobuf;
using Microsoft.AspNetCore.DataProtection.KeyManagement.Internal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using OneOf;
using RaftNET.Services;

namespace RaftNET;

public partial class FSM {
    private ulong _myID;
    private OneOf<Follower, Candidate, Leader> _state = new Follower(0);
    private ulong _currentTerm;
    private ulong _votedFor;
    private ulong _commitIdx;
    private Log _log;
    private FSMConfig _config;
    private bool _abortLeadershipTransfer;
    private bool _pingLeader;
    private FSMOutput _output = new();
    private LogicalClock _clock = new();
    private IFailureDetector _failureDetector;
    private readonly ILogger<FSM> _logger;
    private long _lastElectionTime;
    private long _randomizedElectionTimeout = ElectionTimeout + 1;
    private readonly ThreadLocal<Random> _random = new(() => new Random());
    private List<ToMessage> _messages = new();
    private readonly LastObservedState _observed = new();
    private readonly Notifier _eventNotify;

    public const long ElectionTimeout = 10;

    public FSM(
        ulong id,
        ulong currentTerm,
        ulong votedFor,
        Log log,
        ulong commitIdx,
        IFailureDetector failureDetector,
        FSMConfig config,
        Notifier eventNotify,
        ILogger<FSM>? logger = null
    ) {
        _myID = id;
        _currentTerm = currentTerm;
        _votedFor = votedFor;
        _log = log;
        _failureDetector = failureDetector;
        _config = config;
        _eventNotify = eventNotify;
        _logger = logger ?? new NullLogger<FSM>();

        if (id <= 0) {
            throw new ArgumentException(nameof(id));
        }
        _commitIdx = _log.GetSnapshot().Idx;
        _observed.Advance(this);
        _commitIdx = ulong.Max(_commitIdx, commitIdx);

        if (_log.GetConfiguration().Current.Count == 1 && _log.GetConfiguration().CanVote(_myID)) {
            BecomeCandidate(_config.EnablePreVote);
        } else {
            ResetElectionTimeout();
        }
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
    protected Leader LeaderState => _state.AsT2;

    public bool HasOutput() {
        var diff = _log.LastIdx() - _log.StableIdx();
        return diff > 0 ||
               _output.StateChanged ||
               _messages.Any() ||
               !_observed.Equals(this) ||
               _output.Snapshot != null ||
               _output.SnapshotsToDrop.Any();
    }

    private void Replicate() {
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
            for (var i = _log.StableIdx() + 1; i <= _log.LastIdx(); ++i) {
                output.LogEntries.Add(_log[i]);
            }
        }

        if (_observed.CurrentTerm != _currentTerm || _observed.VotedFor != _votedFor) {
            output.TermAndVote = new TermVote {
                Term = _currentTerm, VotedFor = _votedFor
            };
        }

        var observedCI = ulong.Max(_observed.CommitIdx, _log.GetSnapshot().Idx);

        if (observedCI < _commitIdx) {
            for (var idx = observedCI + 1; idx <= _commitIdx; ++idx) {
                var entry = _log[idx];
                output.Committed.Add(entry);
            }
        }

        output.Messages = _messages;
        _messages = [];

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
            AdvanceStableIdx(output.LogEntries.Last().Idx);
        }

        return output;
    }

    private void AdvanceStableIdx(ulong idx) {
        _log.StableTo(idx);
        if (IsLeader) {
            var leaderProgress = LeaderState.Tracker.Find(_myID);
            if (leaderProgress != null) {
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
        _messages.Add(new ToMessage(to, message));
    }

    private void SendTo(ulong to, VoteRequest request) {
        SendTo(to, new Message(request));
    }

    private void SendTo(ulong to, VoteResponse response) {
        SendTo(to, new Message(response));
    }

    private void SendTo(ulong to, AppendRequest request) {
        SendTo(to, new Message(request));
    }

    private void SendTo(ulong to, AppendResponse response) {
        SendTo(to, new Message(response));
    }

    private void SendTo(ulong to, InstallSnapshot request) {
        SendTo(to, new Message(request));
    }

    private void SendTo(ulong to, SnapshotResponse response) {
        SendTo(to, new Message(response));
    }

    private void SendTo(ulong to, TimeoutNowRequest request) {
        SendTo(to, new Message(request));
    }

    private void UpdateCurrentTerm(ulong term) {
        Debug.Assert(term > _currentTerm);
        _currentTerm = term;
        _votedFor = 0;
    }

    protected void BecomeFollower(ulong leader) {
        if (leader == _myID) {
            throw new FSMException("FSM cannot become a follower of itself");
        }
        if (!IsFollower) {
            _output.StateChanged = true;
        }
        _state = new Follower(leader);
        if (leader != 0) {
            _pingLeader = false;
            _lastElectionTime = _clock.Now();
        }
    }

    private void BecomeLeader() {
        Debug.Assert(!IsLeader);
        _output.StateChanged = true;
        _state = new Leader(_config.MaxLogSize);
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

        if (LeaderState.StepDown != null) {
            // A leader that is stepping down should not add new entries
            // to its log (see 3.10), but it still does not know who the new
            // leader will be.
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

        if (state.StepDown != null) {
            _logger.LogTrace("Tick({}) stepdown is active", _myID);
            var me = state.Tracker.Find(_myID);
            if (me == null || !me.CanVote) {
                _logger.LogTrace("Tick({}) not aborting step down: we have been removed from the configuration", _myID);
            } else if (state.StepDown <= _clock.Now()) {
                _logger.LogTrace("Tick({}) cancel step down", _myID);
                state.StepDown = null;
                state.TimeoutNowSent = null;
                _abortLeadershipTransfer = true;
            } else if (state.TimeoutNowSent != null) {
                _logger.LogTrace("Tick({}) resend TimeoutNowRequest", _myID);
                SendTo(state.TimeoutNowSent.Value, new TimeoutNowRequest { CurrentTerm = _currentTerm });
            }
        }
    }

    private void ReplicateTo(FollowerProgress progress, bool allowEmpty) {
        while (progress.CanSendTo()) {
            var nextIdx = progress.NextIdx;
            if (progress.NextIdx > _log.LastIdx()) {
                nextIdx = 0;
                if (!allowEmpty) {
                    return;
                }
            }

            allowEmpty = false;
            var prevIdx = progress.NextIdx - 1;
            var prevTerm = _log.TermFor(prevIdx);
            if (prevTerm == null) {
                var snapshot = _log.GetSnapshot();
                progress.BecomeSnapshot(snapshot.Idx);
                SendTo(progress.Id, new InstallSnapshot {
                    CurrentTerm = _currentTerm,
                    Snp = snapshot
                });
                return;
            }

            var req = new AppendRequest {
                CurrentTerm = _currentTerm,
                PrevLogIdx = prevIdx,
                PrevLogTerm = prevTerm.Value,
                LeaderCommitIdx = _commitIdx,
            };

            if (nextIdx > 0) {
                var size = 0;
                while (nextIdx <= _log.LastIdx() && size < _config.AppendRequestThreshold) {
                    var entry = _log[nextIdx];
                    req.Entries.Add(entry);
                    size += entry.EntrySize();
                    nextIdx++;
                    if (progress.State == FollowerProgressState.Probe) {
                        break;
                    }
                }

                if (progress.State == FollowerProgressState.Pipeline) {
                    progress.InFlight++;
                    progress.NextIdx = nextIdx;
                }
            } else {
                _logger.LogTrace("ReplicateTo[{}->{}]: send empty", _myID, progress.Id);
            }

            SendTo(progress.Id, req);

            if (progress.State == FollowerProgressState.Probe) {
                progress.ProbeSent = true;
            }
        }
    }

    public void Step(ulong from, VoteRequest msg) { Step(from, new Message(msg)); }
    public void Step(ulong from, VoteResponse msg) { Step(from, new Message(msg)); }
    public void Step(ulong from, AppendRequest msg) { Step(from, new Message(msg)); }
    public void Step(ulong from, AppendResponse msg) { Step(from, new Message(msg)); }
    public void Step(ulong from, InstallSnapshot msg) { Step(from, new Message(msg)); }
    public void Step(ulong from, SnapshotResponse msg) { Step(from, new Message(msg)); }
    public void Step(ulong from, TimeoutNowRequest msg) { Step(from, new Message(msg)); }

    public void Step(ulong from, Message msg) {
        Debug.Assert(from != _myID, "fsm cannot process messages from itself");
        if (msg.CurrentTerm > _currentTerm) {
            ulong leader = 0;
            if (msg.IsAppendRequest || msg.IsInstallSnapshot) {
                leader = from;
            }

            bool ignoreTerm = false;
            if (msg.IsVoteRequest) {
                ignoreTerm = msg.VoteRequest.IsPreVote;
            } else if (msg.IsVoteResponse) {
                var rsp = msg.VoteResponse;
                ignoreTerm = rsp is { IsPreVote: true, VoteGranted: true };
            }

            if (!ignoreTerm) {
                BecomeFollower(leader);
                UpdateCurrentTerm(msg.CurrentTerm);
            }
        } else if (msg.CurrentTerm < _currentTerm) {
            if (msg.IsAppendRequest) {
                SendTo(from, new AppendResponse {
                    CurrentTerm = _currentTerm,
                    CommitIdx = _commitIdx,
                    Rejected = new AppendRejected {
                        LastIdx = _log.LastIdx(),
                        NonMatchingIdx = 0
                    }
                });
            } else if (msg.IsInstallSnapshot) {
                SendTo(from, new SnapshotResponse {
                    CurrentTerm = _currentTerm,
                    Success = false
                });
            } else if (msg.IsVoteRequest) {
                if (msg.VoteRequest.IsPreVote) {
                    SendTo(from, new VoteResponse {
                        CurrentTerm = _currentTerm,
                        VoteGranted = false,
                        IsPreVote = true
                    });
                }
            } else {
                _logger.LogTrace("ignored a message with lower term from {}, term: {}", from, msg.CurrentTerm);
            }
            return;
        } else {
            // _current_term == msg.current_term
            if (msg.IsAppendRequest || msg.IsInstallSnapshot) {
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
                var success = ApplySnapshot(installSnapshot.Snp, 0, 0, false);
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

    public void WaitForMemoryPermit(int size) {
        CheckIsLeader();
        LeaderState.LogLimiter.Wait(size);
    }

    public void TransferLeadership(long timeout = 0) {
        CheckIsLeader();
        var leader = LeaderState.Tracker.Find(_myID);
        var voterCount = GetConfiguration().Current.Count(x => x.CanVote);
        if (voterCount == 1 && leader is { CanVote: true }) {
            throw new NoOtherVotingMemberException();
        }

        LeaderState.StepDown = _clock.Now() + timeout;
        LeaderState.LogLimiter.Wait(_config.MaxLogSize); // prevent new requests
        foreach (var (_, progress) in LeaderState.Tracker.FollowerProgresses) {
            if (progress.Id != _myID && progress.CanVote && progress.MatchIdx == _log.LastIdx()) {
                SendTimeoutNow(progress.Id);
                break;
            }
        }
    }

    private void RequestVote(ulong from, VoteRequest request) {
        Debug.Assert(request.IsPreVote || _currentTerm == request.CurrentTerm);
        var canVote =
            _votedFor == from ||
            _votedFor == 0 && CurrentLeader == 0 ||
            request.IsPreVote && request.CurrentTerm > _currentTerm;

        if (canVote && _log.IsUpToUpdate(request.LastLogIdx, request.LastLogTerm)) {
            if (!request.IsPreVote) {
                _lastElectionTime = _clock.Now();
                _votedFor = from;
            }
            SendTo(from, new VoteResponse {
                CurrentTerm = request.CurrentTerm,
                IsPreVote = request.IsPreVote,
                VoteGranted = true
            });
        } else {
            SendTo(from, new VoteResponse {
                CurrentTerm = request.CurrentTerm,
                IsPreVote = request.IsPreVote,
                VoteGranted = false
            });
        }
    }

    private void RequestVoteResponse(ulong from, VoteResponse rsp) {
        Debug.Assert(IsCandidate);
        var state = CandidateState;
        if (state.IsPreVote != rsp.IsPreVote) {
            return;
        }
        state.Votes.RegisterVote(from, rsp.VoteGranted);

        switch (state.Votes.CountVotes()) {
            case VoteResult.Unknown:
                break;
            case VoteResult.Won:
                if (state.IsPreVote) {
                    BecomeCandidate(false);
                } else {
                    BecomeLeader();
                }
                break;
            case VoteResult.Lost:
                BecomeFollower(0);
                break;
        }
    }


    private void AppendEntries(ulong from, AppendRequest request) {
        Debug.Assert(IsFollower);

        var matchResult = _log.MatchTerm(request.PrevLogIdx, request.PrevLogTerm);
        var match = matchResult.Item1;
        var term = matchResult.Item2;

        if (!match) {
            SendTo(from, new AppendResponse {
                CurrentTerm = _currentTerm,
                CommitIdx = _commitIdx,
                Rejected = new AppendRejected {
                    NonMatchingIdx = request.PrevLogIdx,
                    LastIdx = _log.LastIdx()
                }
            });
            return;
        }

        var lastNewIdx = request.PrevLogIdx;
        if (request.Entries.Count > 0) {
            lastNewIdx = _log.MaybeAppend(request.Entries);
        }
        AdvanceCommitIdx(ulong.Min(request.LeaderCommitIdx, lastNewIdx));
        SendTo(from, new AppendResponse {
            CurrentTerm = _currentTerm,
            CommitIdx = _commitIdx,
            Accepted = new AppendAccepted {
                LastNewIdx = lastNewIdx
            }
        });
    }

    private void AdvanceCommitIdx(ulong leaderCommitIdx) {
        var newCommitIdx = ulong.Min(leaderCommitIdx, _log.LastIdx());
        if (newCommitIdx > _commitIdx) {
            _commitIdx = newCommitIdx;
        }
    }

    private void AppendEntriesResponse(ulong from, AppendResponse response) {
        Debug.Assert(IsLeader);
        var progress = LeaderState.Tracker.Find(from);
        if (progress == null) {
            return;
        }

        if (progress is { State: FollowerProgressState.Pipeline, InFlight: > 0 }) {
            progress.InFlight--;
        }

        if (progress.State == FollowerProgressState.Snapshot) {
            return;
        }

        progress.CommitIdx = ulong.Max(progress.CommitIdx, response.CommitIdx);

        if (response.Accepted != null) {
            var lastIdx = response.Accepted.LastNewIdx;
            progress.Accepted(lastIdx);
            progress.BecomePipeline();
            if (LeaderState.StepDown != null &&
                LeaderState.TimeoutNowSent == null &&
                progress.CanVote &&
                progress.MatchIdx == _log.LastIdx()) {
                SendTimeoutNow(progress.Id);
                if (!IsLeader) {
                    return;
                }
            }
            MaybeCommit();
            if (!IsLeader) {
                return;
            }
        } else {
            var rejected = response.Rejected;
            if (rejected.NonMatchingIdx == 0 && rejected.LastIdx == 0) {
                ReplicateTo(progress, true);
                return;
            }
            if (progress.IsStrayReject(rejected)) {
                return;
            }
            progress.NextIdx = ulong.Min(rejected.NonMatchingIdx, rejected.LastIdx + 1);
            progress.BecomeProbe();
            Debug.Assert(progress.NextIdx > progress.MatchIdx);
        }

        progress = LeaderState.Tracker.Find(from);
        if (progress != null) {
            ReplicateTo(progress, false);
        }
    }

    private void SendTimeoutNow(ulong id) {
        SendTo(id, new TimeoutNowRequest { CurrentTerm = _currentTerm });
        LeaderState.TimeoutNowSent = id;
        var me = LeaderState.Tracker.Find(_myID);
        if (me == null || !me.CanVote) {
            BecomeFollower(0);
        }
    }

    private void InstallSnapshotResponse(ulong from, SnapshotResponse response) {
        var progress = LeaderState.Tracker.Find(from);
        if (progress == null) {
            return;
        }
        if (progress.State != FollowerProgressState.Snapshot) {
            return;
        }
        progress.BecomeProbe();
        if (response.Success) {
            ReplicateTo(progress, false);
        }
    }

    public bool ApplySnapshot(
        SnapshotDescriptor snapshot, int maxTrailingEntries, int maxTrailingBytes, bool local
    ) {
        Debug.Assert(local && snapshot.Idx <= _observed.CommitIdx || !local && IsFollower);
        var currentSnapshot = _log.GetSnapshot();
        if (snapshot.Idx <= currentSnapshot.Idx || (!local && snapshot.Idx <= _commitIdx)) {
            _output.SnapshotsToDrop.Add(snapshot.Id);
            return false;
        }
        _output.SnapshotsToDrop.Add(currentSnapshot.Id);
        _commitIdx = ulong.Max(_commitIdx, snapshot.Idx);
        var newFirstIndex = _log.ApplySnapshot(snapshot, maxTrailingEntries, maxTrailingBytes);
        currentSnapshot = _log.GetSnapshot();
        _output.Snapshot = new AppliedSnapshot(
            currentSnapshot,
            local,
            currentSnapshot.Idx + 1 - newFirstIndex
        );
        return true;
    }

    protected Log GetLog() {
        return _log;
    }

    public Configuration GetConfiguration() {
        return _log.GetConfiguration();
    }
}