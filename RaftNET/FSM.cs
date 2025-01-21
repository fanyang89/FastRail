using System.Diagnostics;
using System.Text;
using Google.Protobuf;
using OneOf;
using RaftNET.Elections;
using RaftNET.Exceptions;
using RaftNET.FailureDetectors;
using RaftNET.Records;
using RaftNET.Replication;
using RaftNET.Services;
using Serilog;

namespace RaftNET;

public partial class FSM {
    public const long ElectionTimeout = 10;
    private readonly LogicalClock _clock = new();
    private readonly Config _config;
    private readonly IFailureDetector _failureDetector;
    private readonly ulong _myID;
    private readonly LastObservedState _observed = new();
    private readonly ThreadLocal<Random> _random = new(() => new Random());
    private readonly Notifier _smEvents;
    private bool _abortLeadershipTransfer;
    private ulong _commitIdx;
    private long _lastElectionTime;
    private List<ToMessage> _messages = new();
    private Output _output = new();
    private bool _pingLeader;
    private long _randomizedElectionTimeout = ElectionTimeout + 1;
    private State _state = new(new Follower(0));
    private ulong _votedFor;

    public FSM(
        ulong id,
        ulong currentTerm,
        ulong votedFor,
        RaftLog log,
        ulong commitIdx,
        IFailureDetector failureDetector,
        Config config,
        Notifier smEvents
    ) {
        _myID = id;
        CurrentTerm = currentTerm;
        _votedFor = votedFor;
        RaftLog = log;
        _failureDetector = failureDetector;
        _config = config;
        _smEvents = smEvents;

        if (id <= 0) {
            throw new ArgumentException("raft::fsm: raft instance cannot have id zero", nameof(id));
        }

        _commitIdx = RaftLog.GetSnapshot().Idx;
        _observed.Advance(this);
        _commitIdx = ulong.Max(_commitIdx, commitIdx);
        Log.Debug("[{my_id}] FSM() starting, current_term={current_term} log_length={log_length} commit_idx={commit_idx}",
            _myID, CurrentTerm, RaftLog.LastIdx(), _commitIdx);

        if (RaftLog.GetConfiguration().Current.Count == 1 && RaftLog.GetConfiguration().CanVote(_myID)) {
            BecomeCandidate(_config.EnablePreVote);
        } else {
            ResetElectionTimeout();
        }
    }

    private Candidate CandidateState => _state.AsT1;
    public ulong CurrentLeader => IsLeader ? _myID : IsFollower ? _state.AsT0.CurrentLeader : 0;
    public string CurrentState => IsLeader ? "Leader" : IsFollower ? "Follower" : "Candidate";
    public ulong CurrentTerm { get; private set; }

    public long ElectionElapsed => _clock.Now() - _lastElectionTime;

    private Follower FollowerState => _state.AsT0;

    public ulong Id => _myID;
    public int InMemoryLogSize => RaftLog.InMemorySize();
    public bool IsCandidate => _state.IsT1;

    public bool IsFollower => _state.IsT0;
    public bool IsLeader => _state.IsT2;
    public bool IsPreVoteCandidate => IsCandidate && _state.AsT1.IsPreVote;
    protected Leader LeaderState => _state.AsT2;
    public ulong LogLastIdx => RaftLog.LastIdx();
    public ulong LogLastTerm => RaftLog.LastTerm();
    public int LogMemoryUsage => RaftLog.MemoryUsage();

    public RaftLog RaftLog { get; }
    public Role Role => IsFollower ? Role.Follower : IsLeader ? Role.Leader : Role.Candidate;

    public LogEntry AddEntry(string command) {
        return AddEntry(Encoding.UTF8.GetBytes(command));
    }

    public LogEntry AddEntry(OneOf<Void, byte[], Configuration> command) {
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
            if (RaftLog.LastConfIdx > _commitIdx || RaftLog.GetConfiguration().IsJoint()) {
                Log.Debug(
                    "[{my_id}] A{joint}configuration change at index {last_conf_idx} is not yet committed (config {config}) (commit_idx: {commit_idx})",
                    _myID, RaftLog.GetConfiguration().IsJoint() ? " joint " : " ",
                    RaftLog.LastConfIdx, RaftLog.GetConfiguration(), _commitIdx);
                throw new ConfigurationChangeInProgressException();
            }
            // 4.3. Arbitrary configuration changes using joint consensus
            //
            // When the leader receives a request to change the
            // configuration from C_old to C_new, it stores the
            // configuration for joint consensus (C_old, new) as a log
            // entry and replicates that entry using the normal Raft
            // mechanism.
            var tmp = RaftLog.GetConfiguration().Clone();
            tmp.EnterJoint(command.AsT2.Current);
            command = tmp;

            Log.Debug("[{my_id}] appending joint config entry at {next_idx}: {command}", _myID, RaftLog.NextIdx(), command);
        }

        var logEntry = new LogEntry { Term = CurrentTerm, Idx = RaftLog.NextIdx() };
        command.Switch(
            _ => { logEntry.Fake = new Void(); },
            buffer => { logEntry.Command = new Command { Buffer = ByteString.CopyFrom(buffer) }; },
            config => { logEntry.Configuration = config; }
        );
        Log.Information("[{my_id}] AddEntry() idx={idx} term={term}", _myID, logEntry.Idx, logEntry.Term);
        RaftLog.Add(logEntry);
        _smEvents.Signal();

        if (isConfig) {
            LeaderState.Tracker.SetConfiguration(RaftLog.GetConfiguration(), RaftLog.LastIdx());
        }

        return RaftLog[RaftLog.LastIdx()];
    }

    public bool ApplySnapshot(
        SnapshotDescriptor snapshot, int maxTrailingEntries, int maxTrailingBytes, bool local
    ) {
        Log.Debug("[{my_id}] ApplySnapshot() current_term={current_term} term={term} idx={idx} id={id} local={local}",
            _myID, CurrentTerm, snapshot.Term, snapshot.Idx, snapshot.Id, local);
        Debug.Assert(local && snapshot.Idx <= _observed.CommitIdx || !local && IsFollower);

        var currentSnapshot = RaftLog.GetSnapshot();
        if (snapshot.Idx <= currentSnapshot.Idx || !local && snapshot.Idx <= _commitIdx) {
            Log.Error(
                "[{my_id}] ApplySnapshot() ignore outdated snapshot {snapshot_id}/{snapshot_idx} current one is {current_snapshot_id}/{current_snapshot_idx}, commit_idx={commit_idx}",
                _myID, snapshot.Id, snapshot.Idx, currentSnapshot.Id, currentSnapshot.Idx, _commitIdx);
            _output.SnapshotsToDrop.Add(snapshot.Id);
            return false;
        }

        _output.SnapshotsToDrop.Add(currentSnapshot.Id);

        _commitIdx = ulong.Max(_commitIdx, snapshot.Idx);
        var (units, newFirstIndex) = RaftLog.ApplySnapshot(snapshot, maxTrailingEntries, maxTrailingBytes);
        currentSnapshot = RaftLog.GetSnapshot();
        _output.Snapshot = new AppliedSnapshot(
            currentSnapshot,
            local,
            currentSnapshot.Idx + 1 - newFirstIndex
        );
        if (IsLeader) {
            Log.Debug("[{my_id}] ApplySnapshot() signal {units} available units", _myID, units);
            LeaderState.LogLimiter.Signal(units);
        }
        _smEvents.Signal();
        return true;
    }

    public Configuration GetConfiguration() {
        return RaftLog.GetConfiguration();
    }

    public Output GetOutput() {
        var diff = RaftLog.LastIdx() - RaftLog.StableIdx();

        if (IsLeader) {
            if (LeaderState.LastReadIdChanged) {
                BroadcastReadQuorum(LeaderState.LastReadId);
                LeaderState.LastReadIdChanged = false;
            }

            if (diff > 0) {
                Replicate();
            }
        }

        var output = _output;
        _output = new Output();

        if (diff > 0) {
            for (var i = RaftLog.StableIdx() + 1; i <= RaftLog.LastIdx(); ++i) {
                output.LogEntries.Add(RaftLog[i]);
            }
        }

        if (_observed.CurrentTerm != CurrentTerm || _observed.VotedFor != _votedFor) {
            output.TermAndVote = new TermVote { Term = CurrentTerm, VotedFor = _votedFor };
        }

        // Return committed entries.
        // Observer commit index may be smaller than snapshot index,
        // in which case we should not attempt to commit entries belonging to a snapshot
        var observedCommitIdx = ulong.Max(_observed.CommitIdx, RaftLog.GetSnapshot().Idx);

        if (observedCommitIdx < _commitIdx) {
            for (var idx = observedCommitIdx + 1; idx <= _commitIdx; ++idx) {
                var entry = RaftLog[idx];
                output.Committed.Add(entry);
            }
        }

        output.Messages = _messages;
        _messages = [];

        output.AbortLeadershipTransfer = _abortLeadershipTransfer;
        _abortLeadershipTransfer = false;

        if (_observed.LastConfIdx != RaftLog.LastConfIdx ||
            _observed.CurrentTerm != RaftLog.LastTerm() &&
            _observed.LastTerm != RaftLog.LastTerm()) {
            output.Configuration = new HashSet<ConfigMember>();
            var lastLogConf = RaftLog.GetConfiguration();

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

    public bool HasOutput() {
        Log.Debug("[{my_id}] FSM.HasOutput() stable_idx={stable_idx} last_idx={last_idx}", _myID, RaftLog.StableIdx(),
            RaftLog.LastIdx());
        var diff = RaftLog.LastIdx() - RaftLog.StableIdx();
        return diff > 0 ||
               _messages.Count != 0 ||
               !_observed.Equals(this) ||
               _output.MaxReadIdWithQuorum.HasValue ||
               IsLeader && LeaderState.LastReadIdChanged ||
               _output.Snapshot != null ||
               _output.SnapshotsToDrop.Any() ||
               _output.StateChanged;
    }

    public void PingLeader() {
        Debug.Assert(CurrentLeader == 0);
        _pingLeader = true;
    }

    public (ulong, ulong)? StartReadBarrier(ulong requester) {
        CheckIsLeader();

        if (requester != _myID && LeaderState.Tracker.Find(requester) == null) {
            throw new OutsideConfigurationException(requester);
        }

        var termForCommitIdx = RaftLog.TermFor(_commitIdx);
        Debug.Assert(termForCommitIdx != null);

        if (termForCommitIdx.Value != CurrentTerm) {
            return null;
        }

        var id = NextReadId();
        Log.Debug("[{my_id}] StartReadBarrier() starting read barrier with id {id}", _myID, id);
        return (id, _commitIdx);
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

    public void Step(ulong from, InstallSnapshotRequest msg) {
        Step(from, new Message(msg));
    }

    public void Step(ulong from, SnapshotResponse msg) {
        Step(from, new Message(msg));
    }

    public void Step(ulong from, TimeoutNowRequest msg) {
        Step(from, new Message(msg));
    }

    public void Step(ulong from, ReadQuorumRequest msg) {
        Step(from, new Message(msg));
    }

    public void Step(ulong from, ReadQuorumResponse msg) {
        Step(from, new Message(msg));
    }

    public void Step(ulong from, Message msg) {
        Debug.Assert(from != _myID, "fsm cannot process messages from itself");

        if (msg.CurrentTerm > CurrentTerm) {
            ulong leader = 0;

            if (msg.IsAppendRequest || msg.IsInstallSnapshotRequest || msg.IsReadQuorumRequest) {
                leader = from;
            } else if (msg.IsReadQuorumResponse) {
                Log.Error("[{my_id}] ignore read barrier response with higher term={term} current_term={current_term}", _myID,
                    msg.CurrentTerm, CurrentTerm);
                return;
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
            if (msg.IsAppendRequest || msg.IsReadQuorumRequest) {
                SendTo(from,
                    new AppendResponse {
                        CurrentTerm = CurrentTerm,
                        CommitIdx = _commitIdx,
                        Rejected = new AppendRejected { LastIdx = RaftLog.LastIdx(), NonMatchingIdx = 0 }
                    });
            } else if (msg.IsInstallSnapshotRequest) {
                SendTo(from, new SnapshotResponse { CurrentTerm = CurrentTerm, Success = false });
            } else if (msg.IsVoteRequest) {
                if (msg.VoteRequest.IsPreVote) {
                    SendTo(from, new VoteResponse { CurrentTerm = CurrentTerm, VoteGranted = false, IsPreVote = true });
                }
            } else {
                Log.Debug("ignored a message with lower term from {from}, term={current_term}", from, msg.CurrentTerm);
            }

            return;
        } else {
            // _current_term == msg.current_term
            if (msg.IsAppendRequest || msg.IsInstallSnapshotRequest || msg.IsReadQuorumRequest) {
                if (IsCandidate) {
                    BecomeFollower(from);
                } else if (CurrentLeader == 0) {
                    FollowerState.CurrentLeader = from;
                    _pingLeader = false;
                }

                _lastElectionTime = _clock.Now();

                if (CurrentLeader != from) {
                    Log.Error(
                        "Got AppendRequest/InstallSnapshot/ReadQuorumRequest from an unexpected leader {from}, expected {currentLeader}",
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

    public void Tick() {
        _clock.Advance();

        if (IsLeader) {
            TickLeader();
        } else if (HasStableLeader()) {
            _lastElectionTime = _clock.Now();
        } else if (ElectionElapsed >= _randomizedElectionTimeout) {
            Log.Debug("[{my_id}] Tick() becoming a candidate at term={term} last_election={last_election} now={now}",
                _myID, CurrentTerm, _lastElectionTime, _clock.Now());
            BecomeCandidate(_config.EnablePreVote);
        }

        if (IsFollower && CurrentLeader == 0 && _pingLeader) {
            var cfg = GetConfiguration();
            if (!cfg.IsJoint() && cfg.Current.Any(x => x.ServerAddress.ServerId == _myID)) {
                foreach (var s in cfg.Current) {
                    if (s.CanVote && s.ServerAddress.ServerId != _myID && _failureDetector.IsAlive(s.ServerAddress.ServerId)) {
                        Log.Debug("[{my_id}] Tick() searching for a leader. Pinging {server_id}", _myID,
                            s.ServerAddress.ServerId);
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

    public void TransferLeadership(long timeout = 0) {
        CheckIsLeader();
        var leader = LeaderState.Tracker.Find(_myID);
        var voterCount = GetConfiguration().Current.Count(x => x.CanVote);

        if (voterCount == 1 && leader is { CanVote: true }) {
            throw new NoOtherVotingMemberException();
        }

        LeaderState.StepDown = _clock.Now() + timeout;
        LeaderState.LogLimiter.Consume(_config.MaxLogSize);

        foreach (var (_, progress) in LeaderState.Tracker.FollowerProgresses) {
            if (progress.Id != _myID && progress.CanVote && progress.MatchIdx == RaftLog.LastIdx()) {
                SendTimeoutNow(progress.Id);
                break;
            }
        }
    }

    private void BroadcastReadQuorum(ulong readId) {
        Log.Debug("[{my_id}] BroadcastReadQuorum() send read id {id}", _myID, readId);
        foreach (var (_, p) in LeaderState.Tracker.FollowerProgresses) {
            if (!p.CanVote) continue;

            if (p.Id == _myID) {
                HandleReadQuorumResponse(_myID, new ReadQuorumResponse {
                    CurrentTerm = CurrentTerm,
                    CommitIdx = _commitIdx,
                    Id = readId
                });
            } else {
                SendTo(p.Id, new ReadQuorumRequest {
                    CurrentTerm = CurrentTerm,
                    LeaderCommitIdx = Math.Min(p.MatchIdx, _commitIdx),
                    Id = readId
                });
            }
        }
    }

    private void Step(ulong from, Leader leader, Message message) {
        message.Switch(
            voteRequest => { HandleVoteRequest(from, voteRequest); },
            voteResponse => {
                // ignored
            },
            appendRequest => {
                // ignored
            },
            appendResponse => { HandleAppendResponse(from, appendResponse); },
            installSnapshot => { SendTo(from, new SnapshotResponse { CurrentTerm = CurrentTerm, Success = false }); },
            snapshotResponse => { HandleSnapshotResponse(from, snapshotResponse); },
            timeoutNowRequest => {
                // ignored
            },
            readQuorumRequest => {
                // ignored
            },
            readQuorumResponse => {
                HandleReadQuorumResponse(from, readQuorumResponse);
            }
        );
    }

    private void HandleReadQuorumResponse(ulong from, ReadQuorumResponse response) {
        Debug.Assert(IsLeader);
        Log.Debug("[{my_id}] HandleReadQuorumResponse() got response, from={from} id={id}", _myID, from, response.Id);

        var state = LeaderState;
        var progress = state.Tracker.Find(from);
        if (progress == null) {
            return;
        }

        progress.CommitIdx = Math.Max(progress.CommitIdx, response.CommitIdx);
        progress.MaxAckedRead = Math.Max(progress.MaxAckedRead, response.Id);

        if (response.Id <= state.MaxReadIdWithQuorum) {
            return;
        }

        var newCommittedRead = LeaderState.Tracker.CommittedReadId(state.MaxReadIdWithQuorum);
        if (newCommittedRead <= state.MaxReadIdWithQuorum) {
            return;
        }

        _output.MaxReadIdWithQuorum = newCommittedRead;
        state.MaxReadIdWithQuorum = newCommittedRead;

        Log.Debug("[{my_id}] HandleReadQuorumResponse() new commit read {new_committed_read}", _myID, newCommittedRead);
        _smEvents.Signal();
    }

    private void Step(ulong from, Candidate candidate, Message message) {
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
            },
            readQuorumRequest => {
                // ignored
            },
            readQuorumResponse => {
                // ignored
            }
        );
    }

    private void Step(ulong from, Follower follower, Message message) {
        message.Switch(
            voteRequest => { HandleVoteRequest(from, voteRequest); },
            voteResponse => {
                // ignored
            },
            appendRequest => { HandleAppendRequest(from, appendRequest); },
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
            timeoutNowRequest => { BecomeCandidate(false, true); },
            readQuorumRequest => {
                Log.Debug("[{my_id}] receive ReadQuorumRequest from {from} for read_id={read_id}", _myID, from,
                    readQuorumRequest.Id);
                AdvanceCommitIdx(readQuorumRequest.LeaderCommitIdx);
                SendTo(from, new ReadQuorumResponse {
                    CurrentTerm = CurrentTerm,
                    CommitIdx = _commitIdx,
                    Id = readQuorumRequest.Id
                });
            },
            readQuorumResponse => {
                // ignored
            }
        );
    }

    private void Replicate() {
        Debug.Assert(IsLeader);

        foreach (var (_, progress) in LeaderState.Tracker.FollowerProgresses) {
            if (progress.Id != _myID) {
                ReplicateTo(progress, false);
            }
        }
    }

    private void AdvanceStableIdx(ulong idx) {
        var prevStableIdx = RaftLog.StableIdx();
        RaftLog.StableTo(idx);
        Log.Debug("[{my_id}] AdvanceStableIdx() prev_stable_idx={prev_stable_idx} idx={idx}", _myID, prevStableIdx, idx);

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

        var committedConfChange = _commitIdx < RaftLog.LastConfIdx && newCommitIdx >= RaftLog.LastConfIdx;

        if (RaftLog[newCommitIdx].Term != CurrentTerm) {
            Log.Debug("[{my_id}] MaybeCommit() cannot commit, because term {new_commit_term} != {current_term}",
                _myID, RaftLog[newCommitIdx].Term, CurrentTerm);
            return;
        }
        Log.Debug("[{my_id}] MaybeCommit() commit_idx={new_commit_idx}", _myID, newCommitIdx);

        _commitIdx = newCommitIdx;
        _smEvents.Signal();

        if (committedConfChange) {
            Log.Debug("[{my_id}] MaybeCommit() committed conf change at idx {log_last_conf_idx} (config: {cfg})", _myID,
                RaftLog.LastConfIdx,
                RaftLog.GetConfiguration());
            if (RaftLog.GetConfiguration().IsJoint()) {
                var cfg = RaftLog.GetConfiguration();
                cfg.LeaveJoint();
                Log.Debug("[{my_id}] MaybeCommit() appending non-joint config entry at {log_next_idx}: {cfg}", _myID,
                    RaftLog.NextIdx(), cfg);
                RaftLog.Add(new LogEntry { Term = CurrentTerm, Idx = RaftLog.NextIdx(), Configuration = cfg });
                LeaderState.Tracker.SetConfiguration(RaftLog.GetConfiguration(), RaftLog.LastIdx());
                MaybeCommit();
            } else {
                var lp = LeaderState.Tracker.Find(_myID);
                if (lp is not { CanVote: true }) {
                    Log.Debug("[{my_id}] MaybeCommit() stepping down as leader", _myID);
                    TransferLeadership();
                }
            }
            if (IsLeader && LeaderState.LastReadId != LeaderState.MaxReadIdWithQuorum) {
                BroadcastReadQuorum(LeaderState.LastReadId);
            }
        }
    }

    private bool HasStableLeader() {
        var cfg = RaftLog.GetConfiguration();
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

    private void SendTo(ulong to, InstallSnapshotRequest request) {
        SendTo(to, new Message(request));
    }

    private void SendTo(ulong to, SnapshotResponse response) {
        SendTo(to, new Message(response));
    }

    private void SendTo(ulong to, TimeoutNowRequest request) {
        SendTo(to, new Message(request));
    }

    private void SendTo(ulong to, ReadQuorumRequest request) {
        SendTo(to, new Message(request));
    }

    private void SendTo(ulong to, ReadQuorumResponse request) {
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

        Log.Information("[{my_id}] BecomeFollower()", _myID);
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
        LeaderState.LogLimiter.Consume(RaftLog.MemoryUsage());
        _lastElectionTime = _clock.Now();
        _pingLeader = false;
        AddEntry(new Void());
        LeaderState.Tracker.SetConfiguration(RaftLog.GetConfiguration(), RaftLog.LastIdx());
        Log.Information("[{my_id}] BecomeLeader() stable_idx={stable_idx} last_idx={last_idx}", _myID, RaftLog.StableIdx(),
            RaftLog.LastIdx());
    }

    private void BecomeCandidate(bool isPreVote, bool isLeadershipTransfer = false) {
        if (!IsCandidate) {
            _output.StateChanged = true;
        }

        if (_state.IsLeader) {
            _state.Leader.Cancel();
        }
        _state = new State(new Candidate(RaftLog.GetConfiguration(), isPreVote));

        ResetElectionTimeout();

        _lastElectionTime = _clock.Now();
        var votes = CandidateState.Votes;
        var voters = votes.Voters;

        if (voters.All(x => x.ServerId != _myID)) {
            if (RaftLog.LastConfIdx <= _commitIdx) {
                BecomeFollower(0);
                return;
            }

            var prevConfig = RaftLog.GetPreviousConfiguration();
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

            Log.Debug(
                "[{my_id}] BecomeCandidate() send vote request to {server_id}, term={term} idx={idx} last_log_term={last_log_term} pre_vote={pre_vote} force={force}",
                _myID, server.ServerId, term, RaftLog.LastIdx(), RaftLog.LastTerm(), isPreVote, isLeadershipTransfer);

            SendTo(server.ServerId,
                new VoteRequest {
                    CurrentTerm = term,
                    Force = isLeadershipTransfer,
                    IsPreVote = isPreVote,
                    LastLogIdx = RaftLog.LastIdx(),
                    LastLogTerm = RaftLog.LastTerm()
                });
        }

        if (votes.CountVotes() == VoteResult.Won) {
            if (isPreVote) {
                Log.Information("[{my_id}] BecomeCandidate() won pre-vote", _myID);
                BecomeCandidate(false);
            } else {
                Log.Information("[{my_id}] BecomeCandidate() won vote", _myID);
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
        var upper = long.Max(ElectionTimeout, RaftLog.GetConfiguration().Current.Count);
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

        foreach (var (_, progress) in state.Tracker.FollowerProgresses) {
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
                        throw new UnreachableException();
                }

                if (progress.MatchIdx < RaftLog.LastIdx() || progress.CommitIdx < _commitIdx) {
                    Log.Debug(
                        "[{my_id}] Tick() replicate to {server_id} because match={match_idx} < last_idx={last_idx} || follower_commit_idx={follower_commit_idx} < commit_idx={commit_idx}",
                        _myID, progress.Id, progress.MatchIdx, RaftLog.LastIdx(),
                        progress.CommitIdx, _commitIdx);
                    ReplicateTo(progress, true);
                }
            }
        }

        if (state.LastReadId != state.MaxReadIdWithQuorum) {
            BroadcastReadQuorum(state.LastReadId);
        }

        if (active.Invoke()) {
            _lastElectionTime = _clock.Now();
        }

        if (state.StepDown != null) {
            Log.Debug("[{my_id}] Tick step down is active", _myID);
            var me = state.Tracker.Find(_myID);

            if (me is not { CanVote: true }) {
                Log.Debug("[{my_id}] Tick() not aborting step down: we have been removed from the configuration", _myID);
            } else if (state.StepDown <= _clock.Now()) {
                Log.Debug("[{my_id}] Tick() cancel step down", _myID);
                // Cancel the step-down (only if the leader is part of the cluster)
                LeaderState.LogLimiter.Signal(_config.MaxLogSize);
                state.StepDown = null;
                state.TimeoutNowSent = null;
                _abortLeadershipTransfer = true;
                _smEvents.Signal();
            } else if (state.TimeoutNowSent != null) {
                Log.Debug("[{my_id}] Tick() resend TimeoutNowRequest", _myID);
                SendTo(state.TimeoutNowSent.Value, new TimeoutNowRequest { CurrentTerm = CurrentTerm });
            }
        }
    }

    private void ReplicateTo(FollowerProgress progress, bool allowEmpty) {
        while (progress.CanSendTo()) {
            var nextIdx = progress.NextIdx;

            if (progress.NextIdx > RaftLog.LastIdx()) {
                nextIdx = 0;

                if (!allowEmpty) {
                    return;
                }
            }

            allowEmpty = false;
            var prevIdx = progress.NextIdx - 1;
            var prevTerm = RaftLog.TermFor(prevIdx);

            if (prevTerm == null) {
                var snapshot = RaftLog.GetSnapshot();
                progress.BecomeSnapshot(snapshot.Idx);
                SendTo(progress.Id, new InstallSnapshotRequest { CurrentTerm = CurrentTerm, Snp = snapshot });
                return;
            }

            var req = new AppendRequest {
                CurrentTerm = CurrentTerm, PrevLogIdx = prevIdx, PrevLogTerm = prevTerm.Value, LeaderCommitIdx = _commitIdx
            };

            if (nextIdx > 0) {
                var size = 0;

                while (nextIdx <= RaftLog.LastIdx() && size < _config.AppendRequestThreshold) {
                    var entry = RaftLog[nextIdx];
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
                Log.Debug("ReplicateTo[{my_id}->{server_id}] send empty", _myID, progress.Id);
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
        if (canVote && RaftLog.IsUpToUpdate(request.LastLogIdx, request.LastLogTerm)) {
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
        Log.Information("[{my_id}] RequestVote() respond to {from}, response={response}", _myID, from, response);
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

    private void HandleAppendRequest(ulong from, AppendRequest request) {
        Debug.Assert(IsFollower);

        var matchResult = RaftLog.MatchTerm(request.PrevLogIdx, request.PrevLogTerm);
        var match = matchResult.Item1;

        if (!match) {
            SendTo(from,
                new AppendResponse {
                    CurrentTerm = CurrentTerm,
                    CommitIdx = _commitIdx,
                    Rejected = new AppendRejected { NonMatchingIdx = request.PrevLogIdx, LastIdx = RaftLog.LastIdx() }
                });
            return;
        }

        var lastNewIdx = request.PrevLogIdx;

        if (request.Entries.Count > 0) {
            lastNewIdx = RaftLog.MaybeAppend(request.Entries);
        }

        AdvanceCommitIdx(ulong.Min(request.LeaderCommitIdx, lastNewIdx));
        SendTo(from,
            new AppendResponse {
                CurrentTerm = CurrentTerm, CommitIdx = _commitIdx, Accepted = new AppendAccepted { LastNewIdx = lastNewIdx }
            });
    }

    private void AdvanceCommitIdx(ulong leaderCommitIdx) {
        var newCommitIdx = ulong.Min(leaderCommitIdx, RaftLog.LastIdx());
        Log.Debug("[{my_id}] AdvanceCommitIdx() leader_commit_idx={leader_commit_idx}, new_commit_idx={new_commit_idx}",
            _myID, leaderCommitIdx, newCommitIdx);
        if (newCommitIdx > _commitIdx) {
            _commitIdx = newCommitIdx;
            _smEvents.Signal();
            Log.Debug("[{my_id}] AdvanceCommitIdx() signal apply_entries, committed: {committed}", _myID, _commitIdx);
        }
    }

    private void HandleAppendResponse(ulong from, AppendResponse response) {
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

            if (LeaderState is { StepDown: not null, TimeoutNowSent: null } &&
                progress.CanVote &&
                progress.MatchIdx == RaftLog.LastIdx()) {
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
        Log.Debug("[{my_id}] SendTimeoutNow() send to {id}", _myID, id);
        SendTo(id, new TimeoutNowRequest { CurrentTerm = CurrentTerm });
        LeaderState.TimeoutNowSent = id;
        var me = LeaderState.Tracker.Find(_myID);
        if (me is { CanVote: true }) {
            return;
        }
        Log.Debug("[{my_id}] SendTimeoutNow() become follower", _myID);
        BecomeFollower(0);
    }

    private void HandleSnapshotResponse(ulong from, SnapshotResponse response) {
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

    private ulong NextReadId() {
        Debug.Assert(IsLeader);
        ++LeaderState.LastReadId;
        LeaderState.LastReadIdChanged = true;
        _smEvents.Signal();
        return LeaderState.LastReadId;
    }
}
