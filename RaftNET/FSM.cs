using System.Diagnostics;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using OneOf;
using RaftNET.Elections;
using RaftNET.Exceptions;
using RaftNET.FailureDetectors;
using RaftNET.Records;
using RaftNET.Replication;
using RaftNET.Services;

namespace RaftNET;

public partial class FSM {
    public const long ElectionTimeout = 10;
    private readonly ulong _myID;
    private readonly Config _config;
    private readonly LogicalClock _clock = new();
    private readonly IFailureDetector _failureDetector;
    private readonly ILogger<FSM> _logger;
    private readonly ThreadLocal<Random> _random = new(() => new Random());
    private readonly LastObservedState _observed = new();
    private readonly Notifier _smEvents;
    private State _state = new(new Follower(0));
    private ulong _votedFor;
    private ulong _commitIdx;
    private bool _abortLeadershipTransfer;
    private bool _pingLeader;
    private Output _output = new();
    private long _lastElectionTime;
    private long _randomizedElectionTimeout = ElectionTimeout + 1;
    private List<ToMessage> _messages = new();

    public FSM(
        ulong id,
        ulong currentTerm,
        ulong votedFor,
        Log log,
        ulong commitIdx,
        IFailureDetector failureDetector,
        Config config,
        Notifier smEvents,
        ILogger<FSM>? logger = null
    ) {
        _myID = id;
        CurrentTerm = currentTerm;
        _votedFor = votedFor;
        Log = log;
        _failureDetector = failureDetector;
        _config = config;
        _smEvents = smEvents;
        _logger = logger ?? new NullLogger<FSM>();

        if (id <= 0) {
            throw new ArgumentException("raft::fsm: raft instance cannot have id zero", nameof(id));
        }

        _commitIdx = Log.GetSnapshot().Idx;
        _observed.Advance(this);
        _commitIdx = ulong.Max(_commitIdx, commitIdx);
        _logger.LogTrace("[{}] FSM() starting, current_term={} log_length={} commit_idx={}", _myID, CurrentTerm,
            Log.LastIdx(), _commitIdx);

        if (Log.GetConfiguration().Current.Count == 1 && Log.GetConfiguration().CanVote(_myID)) {
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
    public ulong CurrentTerm { get; private set; }

    public long ElectionElapsed => _clock.Now() - _lastElectionTime;

    private Follower FollowerState => _state.AsT0;
    private Candidate CandidateState => _state.AsT1;
    protected Leader LeaderState => _state.AsT2;
    public Role Role => IsFollower ? Role.Follower : IsLeader ? Role.Leader : Role.Candidate;

    public Log Log { get; }

    public bool HasOutput() {
        _logger.LogTrace("[{}] FSM.HasOutput() stable_idx={} last_idx={}", _myID, Log.StableIdx(), Log.LastIdx());
        var diff = Log.LastIdx() - Log.StableIdx();
        return diff > 0 ||
               _output.StateChanged ||
               _messages.Count != 0 ||
               !_observed.Equals(this) ||
               _output.Snapshot != null ||
               _output.SnapshotsToDrop.Any();
    }

    public Output GetOutput() {
        var diff = Log.LastIdx() - Log.StableIdx();

        if (IsLeader) {
            if (diff > 0) {
                Replicate();
            }
        }

        var output = _output;
        _output = new Output();

        if (diff > 0) {
            for (var i = Log.StableIdx() + 1; i <= Log.LastIdx(); ++i) {
                output.LogEntries.Add(Log[i]);
            }
        }

        if (_observed.CurrentTerm != CurrentTerm || _observed.VotedFor != _votedFor) {
            output.TermAndVote = new TermVote { Term = CurrentTerm, VotedFor = _votedFor };
        }

        // Return committed entries.
        // Observer commit index may be smaller than snapshot index,
        // in which case we should not attempt to commit entries belonging to a snapshot
        var observedCommitIdx = ulong.Max(_observed.CommitIdx, Log.GetSnapshot().Idx);

        if (observedCommitIdx < _commitIdx) {
            for (var idx = observedCommitIdx + 1; idx <= _commitIdx; ++idx) {
                var entry = Log[idx];
                output.Committed.Add(entry);
            }
        }

        output.Messages = _messages;
        _messages = [];

        output.AbortLeadershipTransfer = _abortLeadershipTransfer;
        _abortLeadershipTransfer = false;

        if (_observed.LastConfIdx != Log.LastConfIdx ||
            _observed.CurrentTerm != Log.LastTerm() &&
            _observed.LastTerm != Log.LastTerm()) {
            output.Configuration = new HashSet<ConfigMember>();
            var lastLogConf = Log.GetConfiguration();

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

    public void Tick() {
        _clock.Advance();

        if (IsLeader) {
            TickLeader();
        } else if (HasStableLeader()) {
            _lastElectionTime = _clock.Now();
        } else if (ElectionElapsed >= _randomizedElectionTimeout) {
            _logger.LogTrace("tick[{}]: becoming a candidate at term {}, last election: {}, now: {}", _myID,
                CurrentTerm, _lastElectionTime, _clock.Now());
            BecomeCandidate(_config.EnablePreVote);
        }

        if (IsFollower && CurrentLeader == 0 && _pingLeader) {
            var cfg = GetConfiguration();
            if (!cfg.IsJoint() && cfg.Current.Any(x => x.ServerAddress.ServerId == _myID)) {
                foreach (var s in cfg.Current) {
                    if (s.CanVote && s.ServerAddress.ServerId != _myID && _failureDetector.IsAlive(s.ServerAddress.ServerId)) {
                        _logger.LogTrace("tick[{}]: searching for a leader. Pinging {}", _myID, s.ServerAddress.ServerId);
                        SendTo(s.ServerAddress.ServerId, new AppendResponse {
                            CurrentTerm = CurrentTerm,
                            CommitIdx = _commitIdx,
                            Rejected = new AppendRejected {
                                NonMatchingIdx = 0,
                                LastIdx = 0
                            }
                        });
                    }
                }
            }
        }
    }

    public LogEntry AddEntry(OneOf<Dummy, byte[], Configuration> command) {
        // It's only possible to add entries on a leader.
        CheckIsLeader();
        if (LeaderState.StepDown != null) {
            // A leader stepping down should not add new entries
            // to its log (see 3.10), but it still does not know who the new
            // leader will be.
            throw new NotLeaderException();
        }

        var isConfig = command.IsT2;
        if (isConfig) {
            // Do not permit changes which would render the cluster
            // unusable, such as transitioning to an empty configuration or
            // one with no voters.
            Messages.CheckConfiguration(command.AsT2.Current);
            if (Log.LastConfIdx > _commitIdx || Log.GetConfiguration().IsJoint()) {
                _logger.LogTrace("[{}] A{}configuration change at index {} is not yet committed (config {}) (commit_idx: {})",
                    _myID, Log.GetConfiguration().IsJoint() ? " joint " : " ",
                    Log.LastConfIdx, Log.GetConfiguration(), _commitIdx);
                throw new ConfigurationChangeInProgressException();
            }
            // 4.3. Arbitrary configuration changes using joint consensus
            //
            // When the leader receives a request to change the
            // configuration from C_old to C_new , it stores the
            // configuration for joint consensus (C_old,new) as a log
            // entry and replicates that entry using the normal Raft
            // mechanism.
            var tmp = Log.GetConfiguration().Clone();
            tmp.EnterJoint(command.AsT2.Current);
            command = tmp;

            _logger.LogTrace("[{}] appending joint config entry at {}: {}", _myID, Log.NextIdx(), command);
        }

        var logEntry = new LogEntry { Term = CurrentTerm, Idx = Log.NextIdx() };
        command.Switch(
            _ => { logEntry.Dummy = new Void(); },
            buffer => { logEntry.Command = new Command { Buffer = ByteString.CopyFrom(buffer) }; },
            config => { logEntry.Configuration = config; }
        );
        _logger.LogInformation("[{}] AddEntry() idx={} term={}", _myID, logEntry.Idx, logEntry.Term);
        Log.Add(logEntry);
        _smEvents.Signal();

        if (isConfig) {
            LeaderState.Tracker.SetConfiguration(Log.GetConfiguration(), Log.LastIdx());
        }

        return Log[Log.LastIdx()];
    }

    public void Step(ulong from, VoteRequest msg) {
        Step(from, new Message(msg));
    }

    public void Step(ulong from, VoteResponse msg) {
        Step(from, new Message(msg));
    }

    public void Step(ulong from, AppendRequest msg) {
        Step(from, new Message(msg));
    }

    public void Step(ulong from, AppendResponse msg) {
        Step(from, new Message(msg));
    }

    public void Step(ulong from, InstallSnapshot msg) {
        Step(from, new Message(msg));
    }

    public void Step(ulong from, SnapshotResponse msg) {
        Step(from, new Message(msg));
    }

    public void Step(ulong from, TimeoutNowRequest msg) {
        Step(from, new Message(msg));
    }

    public void Step(ulong from, Message msg) {
        Debug.Assert(from != _myID, "fsm cannot process messages from itself");

        if (msg.CurrentTerm > CurrentTerm) {
            ulong leader = 0;

            if (msg.IsAppendRequest || msg.IsInstallSnapshot) {
                leader = from;
            }

            var ignoreTerm = false;

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
        } else if (msg.CurrentTerm < CurrentTerm) {
            if (msg.IsAppendRequest) {
                SendTo(from,
                    new AppendResponse {
                        CurrentTerm = CurrentTerm,
                        CommitIdx = _commitIdx,
                        Rejected = new AppendRejected { LastIdx = Log.LastIdx(), NonMatchingIdx = 0 }
                    });
            } else if (msg.IsInstallSnapshot) {
                SendTo(from, new SnapshotResponse { CurrentTerm = CurrentTerm, Success = false });
            } else if (msg.IsVoteRequest) {
                if (msg.VoteRequest.IsPreVote) {
                    SendTo(from, new VoteResponse { CurrentTerm = CurrentTerm, VoteGranted = false, IsPreVote = true });
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
                    _logger.LogError(
                        "Got append request/install snapshot/read_quorum from an unexpected leader {from}, expected {currentLeader}",
                        from, CurrentLeader);
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
            voteRequest => { HandleVoteRequest(from, voteRequest); },
            voteResponse => {
                // ignored
            },
            appendRequest => {
                // ignored
            },
            appendResponse => { AppendEntriesResponse(from, appendResponse); },
            installSnapshot => { SendTo(from, new SnapshotResponse { CurrentTerm = CurrentTerm, Success = false }); },
            snapshotResponse => { InstallSnapshotResponse(from, snapshotResponse); },
            timeoutNowRequest => {
                // ignored
            }
        );
    }

    public void Step(ulong from, Candidate candidate, Message message) {
        message.Switch(
            voteRequest => { HandleVoteRequest(from, voteRequest); },
            voteResponse => { HandleVoteResponse(from, voteResponse); },
            appendRequest => {
                // ignored
            },
            appendResponse => {
                // ignored
            },
            installSnapshot => { SendTo(from, new SnapshotResponse { CurrentTerm = CurrentTerm, Success = false }); },
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
            voteRequest => { HandleVoteRequest(from, voteRequest); },
            voteResponse => {
                // ignored
            },
            appendRequest => { AppendEntries(from, appendRequest); },
            appendResponse => {
                // ignored
            },
            installSnapshot => {
                var success = ApplySnapshot(installSnapshot.Snp, 0, 0, false);
                SendTo(from, new SnapshotResponse { CurrentTerm = CurrentTerm, Success = success });
            },
            snapshotResponse => {
                // ignored
            },
            timeoutNowRequest => { BecomeCandidate(false, true); }
        );
    }

    public void TransferLeadership(long timeout = 0) {
        // TODO: prevent new requests coming in
        CheckIsLeader();
        var leader = LeaderState.Tracker.Find(_myID);
        var voterCount = GetConfiguration().Current.Count(x => x.CanVote);

        if (voterCount == 1 && leader is { CanVote: true }) {
            throw new NoOtherVotingMemberException();
        }

        LeaderState.StepDown = _clock.Now() + timeout;
        LeaderState.LogLimiter.Consume(_config.MaxLogSize);

        foreach (var (_, progress) in LeaderState.Tracker.FollowerProgresses) {
            if (progress.Id != _myID && progress.CanVote && progress.MatchIdx == Log.LastIdx()) {
                SendTimeoutNow(progress.Id);
                break;
            }
        }
    }

    public bool ApplySnapshot(
        SnapshotDescriptor snapshot, int maxTrailingEntries, int maxTrailingBytes, bool local
    ) {
        _logger.LogTrace("[{}] ApplySnapshot() current_term={} term={} idx={} id={} local={}",
            _myID, CurrentTerm, snapshot.Term, snapshot.Idx, snapshot.Id, local);
        Debug.Assert(local && snapshot.Idx <= _observed.CommitIdx || !local && IsFollower);

        var currentSnapshot = Log.GetSnapshot();
        if (snapshot.Idx <= currentSnapshot.Idx || !local && snapshot.Idx <= _commitIdx) {
            _logger.LogError("[{}] ApplySnapshot() ignore outdated snapshot {}/{} current one is {}/{}, commit_idx={}",
                _myID, snapshot.Id, snapshot.Idx, currentSnapshot.Id, currentSnapshot.Idx, _commitIdx);
            _output.SnapshotsToDrop.Add(snapshot.Id);
            return false;
        }

        _output.SnapshotsToDrop.Add(currentSnapshot.Id);

        _commitIdx = ulong.Max(_commitIdx, snapshot.Idx);
        var (units, newFirstIndex) = Log.ApplySnapshot(snapshot, maxTrailingEntries, maxTrailingBytes);
        currentSnapshot = Log.GetSnapshot();
        _output.Snapshot = new AppliedSnapshot(
            currentSnapshot,
            local,
            currentSnapshot.Idx + 1 - newFirstIndex
        );
        if (IsLeader) {
            _logger.LogTrace("[{}] ApplySnapshot() signal {} available units", _myID, units);
            LeaderState.LogLimiter.Signal(units);
        }
        _smEvents.Signal();
        return true;
    }

    public Configuration GetConfiguration() {
        return Log.GetConfiguration();
    }

    private void Replicate() {
        Debug.Assert(IsLeader);

        foreach (var (id, progress) in LeaderState.Tracker.FollowerProgresses) {
            if (progress.Id != _myID) {
                ReplicateTo(progress, false);
            }
        }
    }

    private void AdvanceStableIdx(ulong idx) {
        var prevStableIdx = Log.StableIdx();
        Log.StableTo(idx);
        _logger.LogTrace("[{}] AdvanceStableIdx() prev_stable_idx={} idx={}", _myID, prevStableIdx, idx);

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

        var committedConfChange = _commitIdx < Log.LastConfIdx && newCommitIdx >= Log.LastConfIdx;

        if (Log[newCommitIdx].Term != CurrentTerm) {
            _logger.LogTrace("[{}] MaybeCommit() cannot commit, because term {} != {}", _myID, Log[newCommitIdx].Term,
                CurrentTerm);
            return;
        }
        _logger.LogTrace("[{}] MaybeCommit() commit_idx={}", _myID, newCommitIdx);

        _commitIdx = newCommitIdx;
        _smEvents.Signal();

        if (committedConfChange) {
            _logger.LogTrace("[{}] MaybeCommit() committed conf change at idx {} (config: {})", _myID, Log.LastConfIdx,
                Log.GetConfiguration());
            if (Log.GetConfiguration().IsJoint()) {
                var cfg = Log.GetConfiguration();
                cfg.LeaveJoint();
                _logger.LogTrace("[{}] MaybeCommit() appending non-joint config entry at {}: {}", _myID, Log.NextIdx(), cfg);
                Log.Add(new LogEntry { Term = CurrentTerm, Idx = Log.NextIdx(), Configuration = cfg });
                LeaderState.Tracker.SetConfiguration(Log.GetConfiguration(), Log.LastIdx());
                MaybeCommit();
            } else {
                var lp = LeaderState.Tracker.Find(_myID);
                if (lp == null || !lp.CanVote) {
                    _logger.LogTrace("[{}] MaybeCommit() stepping down as leader", _myID);
                    TransferLeadership();
                }
            }
        }
    }

    private bool HasStableLeader() {
        var cfg = Log.GetConfiguration();
        var currentLeader = CurrentLeader;
        return currentLeader > 0 && cfg.CanVote(currentLeader) && _failureDetector.IsAlive(currentLeader);
    }

    private void SendTo(ulong to, Message message) {
        _messages.Add(new ToMessage(to, message));
        _smEvents.Signal();
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
        Debug.Assert(term > CurrentTerm);
        CurrentTerm = term;
        _votedFor = 0;
    }

    protected void BecomeFollower(ulong leader) {
        if (leader == _myID) {
            throw new FSMException("FSM cannot become a follower of itself");
        }

        if (!IsFollower) {
            _output.StateChanged = true;
        }

        _logger.LogInformation("[{}] BecomeFollower()", _myID);
        if (_state.IsLeader) {
            _state.Leader.Cancel();
        }
        _state = new State(new Follower(leader));

        if (leader != 0) {
            _pingLeader = false;
            _lastElectionTime = _clock.Now();
        }
    }

    private void BecomeLeader() {
        Debug.Assert(!IsLeader);
        _output.StateChanged = true;
        _state = new State(new Leader(_config.MaxLogSize));
        LeaderState.LogLimiter.Consume(Log.MemoryUsage());
        _lastElectionTime = _clock.Now();
        _pingLeader = false;
        AddEntry(new Dummy());
        LeaderState.Tracker.SetConfiguration(Log.GetConfiguration(), Log.LastIdx());
        _logger.LogInformation("[{}] BecomeLeader() stable_idx={} last_idx={}", _myID, Log.StableIdx(), Log.LastIdx());
    }

    private void BecomeCandidate(bool isPreVote, bool isLeadershipTransfer = false) {
        if (!IsCandidate) {
            _output.StateChanged = true;
        }

        if (_state.IsLeader) {
            _state.Leader.Cancel();
        }
        _state = new State(new Candidate(Log.GetConfiguration(), isPreVote));

        ResetElectionTimeout();

        _lastElectionTime = _clock.Now();
        var votes = CandidateState.Votes;
        var voters = votes.Voters;

        if (voters.All(x => x.ServerId != _myID)) {
            if (Log.LastConfIdx <= _commitIdx) {
                BecomeFollower(0);
                return;
            }

            var prevConfig = Log.GetPreviousConfiguration();
            Debug.Assert(prevConfig != null);

            if (!prevConfig.CanVote(_myID)) {
                BecomeFollower(0);
                return;
            }
        }

        var term = CurrentTerm + 1;

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

            _logger.LogTrace(
                "[{}] BecomeCandidate() send vote request to {}, term={} idx={} last_log_term={} pre_vote={} force={}",
                _myID, server.ServerId, term, Log.LastIdx(), Log.LastTerm(), isPreVote, isLeadershipTransfer);

            SendTo(server.ServerId,
                new VoteRequest {
                    CurrentTerm = term,
                    Force = isLeadershipTransfer,
                    IsPreVote = isPreVote,
                    LastLogIdx = Log.LastIdx(),
                    LastLogTerm = Log.LastTerm()
                });
        }

        if (votes.CountVotes() == VoteResult.Won) {
            if (isPreVote) {
                _logger.LogInformation("[{}] BecomeCandidate() won pre-vote", _myID);
                BecomeCandidate(false);
            } else {
                _logger.LogInformation("[{}] BecomeCandidate() won vote", _myID);
                BecomeLeader();
            }
        }
    }

    private void CheckIsLeader() {
        if (!IsLeader) {
            throw new NotLeaderException();
        }
    }

    private void ResetElectionTimeout() {
        Debug.Assert(_random.Value != null);
        var upper = long.Max(ElectionTimeout, Log.GetConfiguration().Current.Count);
        _randomizedElectionTimeout = ElectionTimeout + _random.Value.NextInt64(1, upper);
    }

    private void TickLeader() {
        if (ElectionElapsed >= ElectionTimeout) {
            BecomeFollower(0);
            return;
        }

        var state = LeaderState;
        var active = state.Tracker.GetActivityTracker();
        active.Invoke(_myID);

        foreach (var (id, progress) in state.Tracker.FollowerProgresses) {
            if (progress.Id != _myID) {
                if (_failureDetector.IsAlive(progress.Id)) {
                    active.Invoke(progress.Id);
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

                if (progress.MatchIdx < Log.LastIdx() || progress.CommitIdx < _commitIdx) {
                    _logger.LogTrace(
                        "tick[{}]: replicate to {} because match={} < last_idx={} || follower commit_idx={} < commit_idx={}",
                        _myID, progress.Id, progress.MatchIdx, Log.LastIdx(),
                        progress.CommitIdx, _commitIdx);
                    ReplicateTo(progress, true);
                }
            }
        }

        if (active.Invoke()) {
            _lastElectionTime = _clock.Now();
        }

        if (state.StepDown != null) {
            _logger.LogTrace("Tick({}) stepdown is active", _myID);
            var me = state.Tracker.Find(_myID);

            if (me == null || !me.CanVote) {
                _logger.LogTrace("Tick({}) not aborting step down: we have been removed from the configuration", _myID);
            } else if (state.StepDown <= _clock.Now()) {
                _logger.LogTrace("Tick({}) cancel step down", _myID);
                // Cancel the step-down (only if the leader is part of the cluster)
                LeaderState.LogLimiter.Signal(_config.MaxLogSize);
                state.StepDown = null;
                state.TimeoutNowSent = null;
                _abortLeadershipTransfer = true;
                _smEvents.Signal();
            } else if (state.TimeoutNowSent != null) {
                _logger.LogTrace("Tick({}) resend TimeoutNowRequest", _myID);
                SendTo(state.TimeoutNowSent.Value, new TimeoutNowRequest { CurrentTerm = CurrentTerm });
            }
        }
    }

    private void ReplicateTo(FollowerProgress progress, bool allowEmpty) {
        while (progress.CanSendTo()) {
            var nextIdx = progress.NextIdx;

            if (progress.NextIdx > Log.LastIdx()) {
                nextIdx = 0;

                if (!allowEmpty) {
                    return;
                }
            }

            allowEmpty = false;
            var prevIdx = progress.NextIdx - 1;
            var prevTerm = Log.TermFor(prevIdx);

            if (prevTerm == null) {
                var snapshot = Log.GetSnapshot();
                progress.BecomeSnapshot(snapshot.Idx);
                SendTo(progress.Id, new InstallSnapshot { CurrentTerm = CurrentTerm, Snp = snapshot });
                return;
            }

            var req = new AppendRequest {
                CurrentTerm = CurrentTerm, PrevLogIdx = prevIdx, PrevLogTerm = prevTerm.Value, LeaderCommitIdx = _commitIdx
            };

            if (nextIdx > 0) {
                var size = 0;

                while (nextIdx <= Log.LastIdx() && size < _config.AppendRequestThreshold) {
                    var entry = Log[nextIdx];
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

    private void HandleVoteRequest(ulong from, VoteRequest request) {
        Debug.Assert(request.IsPreVote || CurrentTerm == request.CurrentTerm);
        var canVote =
            _votedFor == from ||
            _votedFor == 0 && CurrentLeader == 0 ||
            request.IsPreVote && request.CurrentTerm > CurrentTerm;

        VoteResponse response;
        if (canVote && Log.IsUpToUpdate(request.LastLogIdx, request.LastLogTerm)) {
            if (!request.IsPreVote) {
                _lastElectionTime = _clock.Now();
                _votedFor = from;
            }

            response = new VoteResponse {
                CurrentTerm = request.CurrentTerm,
                IsPreVote = request.IsPreVote,
                VoteGranted = true
            };
        } else {
            response = new VoteResponse {
                CurrentTerm = CurrentTerm,
                IsPreVote = request.IsPreVote,
                VoteGranted = false
            };
        }
        _logger.LogInformation("[{}] RequestVote() respond to {}, response={}", _myID, from, response);
        SendTo(from, response);
    }

    private void HandleVoteResponse(ulong from, VoteResponse response) {
        Debug.Assert(IsCandidate);
        var state = CandidateState;

        if (state.IsPreVote != response.IsPreVote) {
            return;
        }

        state.Votes.RegisterVote(from, response.VoteGranted);

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
            default:
                throw new UnreachableException();
        }
    }

    private void AppendEntries(ulong from, AppendRequest request) {
        Debug.Assert(IsFollower);

        var matchResult = Log.MatchTerm(request.PrevLogIdx, request.PrevLogTerm);
        var match = matchResult.Item1;
        var term = matchResult.Item2;

        if (!match) {
            SendTo(from,
                new AppendResponse {
                    CurrentTerm = CurrentTerm,
                    CommitIdx = _commitIdx,
                    Rejected = new AppendRejected { NonMatchingIdx = request.PrevLogIdx, LastIdx = Log.LastIdx() }
                });
            return;
        }

        var lastNewIdx = request.PrevLogIdx;

        if (request.Entries.Count > 0) {
            lastNewIdx = Log.MaybeAppend(request.Entries);
        }

        AdvanceCommitIdx(ulong.Min(request.LeaderCommitIdx, lastNewIdx));
        SendTo(from,
            new AppendResponse {
                CurrentTerm = CurrentTerm, CommitIdx = _commitIdx, Accepted = new AppendAccepted { LastNewIdx = lastNewIdx }
            });
    }

    private void AdvanceCommitIdx(ulong leaderCommitIdx) {
        var newCommitIdx = ulong.Min(leaderCommitIdx, Log.LastIdx());
        _logger.LogTrace("[{}] AdvanceCommitIdx() leader_commit_idx={}, new_commit_idx={}",
            _myID, leaderCommitIdx, newCommitIdx);
        if (newCommitIdx > _commitIdx) {
            _commitIdx = newCommitIdx;
            _smEvents.Signal();
            _logger.LogTrace("[{}] AdvanceCommitIdx() signal apply_entries: committed: {}",
                _myID, _commitIdx);
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
                progress.MatchIdx == Log.LastIdx()) {
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
        SendTo(id, new TimeoutNowRequest { CurrentTerm = CurrentTerm });
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

    protected Log GetLog() {
        return Log;
    }

    public ulong Id => _myID;

    public int InMemoryLogSize => Log.InMemorySize();
    public int LogMemoryUsage => Log.MemoryUsage();
    public ulong LogLastIdx => Log.LastIdx();
    public ulong LogLastTerm => Log.LastTerm();
}
