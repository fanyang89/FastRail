using System.Collections.Concurrent;
using System.Diagnostics;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using OneOf;
using RaftNET.Exceptions;
using RaftNET.FailureDetectors;
using RaftNET.Persistence;
using RaftNET.Records;
using RaftNET.StateMachines;

namespace RaftNET.Services;

public class RaftService : IRaftRpcHandler {
    private readonly FSM _fsm;
    private readonly Notifier _fsmEventNotify;
    private readonly ILogger<RaftService> _logger;
    private readonly IStateMachine _stateMachine;
    private readonly IPersistence _persistence;
    private readonly BlockingCollection<ApplyMessage> _applyMessages = new();
    private readonly IRaftRpcClient _connectionManager;
    private readonly AddressBook _addressBook;
    private readonly ulong _myId;
    private readonly RaftServiceOptions _options;
    private readonly OrderedDictionary<TermIdx, Notifier> _applyNotifiers = new();
    private readonly OrderedDictionary<TermIdx, Notifier> _commitNotifiers = new();
    private ulong _appliedIdx;
    private ulong _snapshotDescIdx;
    private readonly Dictionary<ulong, TaskCompletionSource<SnapshotResponse>> _snapshotResponsePromises = new();
    private Timer? _ticker;
    private Task? _applyTask;
    private Task? _ioTask;
    private readonly ILoggerFactory _loggerFactory;

    public RaftService(ulong myId, IRaftRpcClient rpc, IStateMachine sm, IPersistence persistence,
        IFailureDetector fd, AddressBook addressBook, ILoggerFactory loggerFactory, RaftServiceOptions options) {
        _myId = myId;
        _stateMachine = sm;
        _addressBook = addressBook;
        _logger = loggerFactory.CreateLogger<RaftService>();
        _connectionManager = new ConnectionManager(_myId, _addressBook, loggerFactory.CreateLogger<ConnectionManager>());
        _persistence = persistence;
        _fsmEventNotify = new Notifier();
        _loggerFactory = loggerFactory;

        _logger.LogInformation("Raft service initializing");

        ulong term = 0;
        ulong votedFor = 0;
        var tv = _persistence.LoadTermVote();

        if (tv != null) {
            term = tv.Term;
            votedFor = tv.VotedFor;
        }

        var commitedIdx = _persistence.LoadCommitIdx();
        var snapshot = _persistence.LoadSnapshotDescriptor();

        if (snapshot == null) {
            var members = _addressBook.GetMembers();
            _logger.LogInformation("Load empty snapshot, get {} initial members from current address book", members.Count);
            snapshot = new SnapshotDescriptor { Config = Messages.ConfigFromIds(members) };
        }

        var logEntries = _persistence.LoadLog();
        var log = new Log(snapshot, logEntries);
        var fsmConfig = new FSM.Config(
            options.EnablePreVote,
            options.AppendRequestThreshold,
            options.MaxLogSize
        );
        _fsm = new FSM(
            _myId, term, votedFor, log, commitedIdx, fd, fsmConfig, _fsmEventNotify,
            _loggerFactory.CreateLogger<FSM>());

        if (snapshot is { Id: > 0 }) {
            _stateMachine.LoadSnapshot(snapshot.Id);
            _snapshotDescIdx = snapshot.Idx;
            _appliedIdx = snapshot.Idx;
        }
    }

    public Task StartAsync(CancellationToken token) {
        _ticker = new Timer(Tick, null, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(100));
        _applyTask = Task.Run(DoApply(token), token);
        _ioTask = Task.Run(DoIO(token, 0), token);
        _logger.LogInformation("RaftService{{{}}} started", _myId);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken) {
        if (_ioTask != null) {
            await _ioTask.WaitAsync(cancellationToken);
        }

        _applyMessages.Add(new ApplyMessage(), cancellationToken);

        if (_applyTask != null) {
            await _applyTask.WaitAsync(cancellationToken);
        }

        if (_ticker != null) {
            await _ticker.DisposeAsync();
        }

        _logger.LogInformation("RaftService{{{}}} stopped", _myId);
    }

    public T AcquireFSMLock<T>(Func<FSM, T> fn) {
        lock (_fsm) {
            return fn(_fsm);
        }
    }

    public void AddEntry(ulong command, WaitType waitType) {
        var buffer = BitConverter.GetBytes(command);
        AddEntry(buffer, waitType);
    }

    public void AddEntry(OneOf<byte[], Configuration> command, WaitType waitType) {
        var commandLength = command.Match(
            buffer => buffer.Length,
            configuration => configuration.CalculateSize());

        if (commandLength >= _options.MaxCommandSize) {
            throw new CommandTooLargeException(commandLength, _options.MaxCommandSize);
        }

        LogEntry entry;

        lock (_fsm) {
            if (!_fsm.IsLeader) {
                throw new NotLeaderException();
            }

            if (command.IsT0) {
                entry = _fsm.AddEntry(command.AsT0);
            } else {
                entry = _fsm.AddEntry(command.AsT1);
            }
        }

        WaitForEntry(entry, waitType);
    }

    public void AddEntry(byte[] buffer) {
        AddEntry(buffer, WaitType.Committed);
    }

    public void AddEntryApplied(byte[] buffer) {
        AddEntry(buffer, WaitType.Applied);
    }

    public void AddEntry(Configuration configuration) {
        AddEntry(configuration, WaitType.Committed);
    }

    private Action DoApply(CancellationToken cancellationToken) {
        return () => {
            _logger.LogInformation("Apply{{{}}} started", _myId);

            while (!cancellationToken.IsCancellationRequested) {
                var message = _applyMessages.Take();

                if (message.IsExit) {
                    break;
                }

                message.Switch(
                    entries => {
                        _logger.LogTrace("Apply{{{}}} on apply {} entries", _myId, entries.Count);

                        lock (_commitNotifiers) {
                            NotifyWaiters(_commitNotifiers, entries);
                        }

                        _logger.LogTrace("Apply({}) applying...", _myId);
                        _stateMachine.Apply(entries
                            .Where(x => x.DataCase == LogEntry.DataOneofCase.Command)
                            .Select(x => x.Command)
                            .ToList()
                        );

                        lock (_applyNotifiers) {
                            NotifyWaiters(_applyNotifiers, entries);
                        }

                        _appliedIdx = entries.Last().Idx;
                    },
                    snapshot => {
                        Debug.Assert(snapshot.Idx >= _appliedIdx);
                        _logger.LogInformation("Applying snapshot {}", snapshot.Id);
                        _stateMachine.LoadSnapshot(snapshot.Id);
                        _appliedIdx = snapshot.Idx;
                    },
                    _ => {}
                );
            }

            _logger.LogInformation("Apply{{{}}} stopped", _myId);
        };
    }

    private void NotifyWaiters(OrderedDictionary<TermIdx, Notifier> waiters, List<LogEntry> entries) {
        var firstIdx = entries.First().Idx;
        var commitIdx = entries.Last().Idx;
        var commitTerm = entries.Last().Term;

        while (waiters.Count > 0) {
            var waiter = waiters.First();

            if (waiter.Key.Idx > commitIdx) {
                break;
            }

            var idx = waiter.Key.Idx;
            var term = waiter.Key.Term;
            var notifier = waiter.Value;
            Debug.Assert(idx >= firstIdx);
            waiters.Remove(waiter.Key);

            if (term == entries[(int)(idx - firstIdx)].Term) {
                notifier.Signal();
            } else {
                throw new DroppedEntryException();
            }
        }

        while (waiters.Count > 0) {
            var waiter = waiters.First();

            if (waiter.Key.Term < commitTerm) {
                throw new DroppedEntryException();
            }
            break;
        }
    }

    private Func<Task> DoIO(CancellationToken cancellationToken, ulong stableIdx) {
        return async () => {
            _logger.LogInformation("IO{{{}}} started", _myId);

            while (!cancellationToken.IsCancellationRequested) {
                _fsmEventNotify.Wait();
                FSM.Output? batch = null;

                lock (_fsm) {
                    var hasOutput = _fsm.HasOutput();

                    if (hasOutput) {
                        batch = _fsm.GetOutput();
                    }
                }

                if (batch != null) {
                    _logger.LogInformation("Processing fsm output, count={}", batch.LogEntries.Count);
                    await ProcessFSMOutput(stableIdx, batch);
                }
            }

            _logger.LogInformation("IO{{{}}} started", _myId);
        };
    }

    private async Task ProcessFSMOutput(ulong lastStable, FSM.Output batch) {
        if (batch.TermAndVote != null) {
            var term = batch.TermAndVote.Term;
            var vote = batch.TermAndVote.VotedFor;
            _persistence.StoreTermVote(term, vote);
        }

        if (batch.Snapshot != null) {
            var snp = batch.Snapshot.Snapshot;
            var isLocal = batch.Snapshot.IsLocal;
            var preservedLogEntries = batch.Snapshot.PreservedLogEntries;
            _persistence.StoreSnapshotDescriptor(snp, preservedLogEntries);
            _snapshotDescIdx = snp.Idx;

            if (!isLocal) {
                _applyMessages.Add(new ApplyMessage(snp));
            }
        }

        foreach (var snp in batch.SnapshotsToDrop) {
            _stateMachine.DropSnapshot(snp);
        }

        if (batch.LogEntries.Count > 0) {
            var entries = batch.LogEntries;

            if (lastStable >= entries.First().Idx) {
                _persistence.TruncateLog(entries.First().Idx);
            }

            _persistence.StoreLogEntries(entries);
            lastStable = entries.Last().Idx;
        }

        foreach (var message in batch.Messages) {
            await SendMessage(message.To, message.Message);
        }

        if (batch.Committed.Count > 0) {
            _persistence.StoreCommitIdx(batch.Committed.Last().Idx);
            _applyMessages.Add(new ApplyMessage(batch.Committed));
        }
    }

    private async Task SendMessage(ulong to, Message message) {
        if (message.IsInstallSnapshotRequest) {
            SnapshotResponse? response = null;
            try {
                response = await _connectionManager.SendSnapshotAsync(to, message.InstallSnapshotRequest);
            }
            catch (RpcException ex) {
                _logger.LogError("[{}] Failed to send snapshot, to={} ex={} detail=\"{}\"",
                    _myId, to, ex.StatusCode, ex.Status.Detail);
            }
            lock (_fsm) {
                _fsm.Step(to, response ?? new SnapshotResponse { CurrentTerm = _fsm.CurrentTerm, Success = false });
            }
            return;
        }

        if (message.IsSnapshotResponse) {
            lock (_snapshotResponsePromises) {
                Debug.Assert(_snapshotResponsePromises.ContainsKey(to));
                _snapshotResponsePromises[to].SetResult(message.SnapshotResponse);
                _snapshotResponsePromises.Remove(to);
            }
            return;
        }

        try {
            if (message.IsVoteRequest) {
                await _connectionManager.VoteRequestAsync(to, message.VoteRequest);
            } else if (message.IsVoteResponse) {
                await _connectionManager.VoteResponseAsync(to, message.VoteResponse);
            } else if (message.IsAppendRequest) {
                await _connectionManager.AppendRequestAsync(to, message.AppendRequest);
            } else if (message.IsAppendResponse) {
                await _connectionManager.AppendResponseAsync(to, message.AppendResponse);
            } else if (message.IsReadQuorumRequest) {
                await _connectionManager.ReadQuorumRequestAsync(to, message.ReadQuorumRequest);
            } else if (message.IsReadQuorumResponse) {
                await _connectionManager.ReadQuorumResponseAsync(to, message.ReadQuorumResponse);
            } else if (message.IsTimeoutNowRequest) {
                await _connectionManager.TimeoutNowRequestAsync(to, message.TimeoutNowRequest);
            } else {
                throw new UnreachableException("Unknown message type");
            }
        }
        catch (RpcException ex) {
            _logger.LogError("[{}] Failed to send message, message={} to={} ex={} detail=\"{}\"",
                _myId, message.Name, to, ex.StatusCode, ex.Status.Detail);
        }
    }

    public void Tick(object? state = null) {
        lock (_fsm) {
            _fsm.Tick();
        }
    }

    private void WaitForEntry(LogEntry entry, WaitType waitType) {
        var termIndex = new TermIdx(entry.Idx, entry.Term);
        var ok = false;
        var notifier = new Notifier();

        switch (waitType) {
            case WaitType.Committed:
                lock (_commitNotifiers) {
                    ok = _commitNotifiers.TryAdd(termIndex, notifier);
                }

                break;
            case WaitType.Applied:
                lock (_applyNotifiers) {
                    ok = _applyNotifiers.TryAdd(termIndex, notifier);
                }

                break;
        }

        Debug.Assert(ok);
        notifier.Wait();
    }

    public void WaitUntilCandidate() {
        lock (_fsm) {
            while (_fsm.IsFollower) {
                _fsm.Tick();
            }
        }
    }

    public async Task WaitElectionDone() {
        lock (_fsm) {
            while (_fsm.IsCandidate) {
                throw new NotImplementedException();
            }
        }
    }

    public (ulong, ulong) LogLastIdxTerm() {
        lock (_fsm) {
            return (_fsm.LogLastIdx, _fsm.LogLastTerm);
        }
    }

    public async Task WaitLogIdxTerm((ulong, ulong) leaderLogIdxTerm) {
        throw new NotImplementedException();
    }

    public bool IsLeader() {
        lock (_fsm) {
            return _fsm.IsLeader;
        }
    }

    public void ElapseElection() {
        lock (_fsm) {
            while (_fsm.ElectionElapsed < FSM.ElectionTimeout) {
                _fsm.Tick();
            }
        }
    }

    public Task HandleVoteRequestAsync(ulong from, VoteRequest message) {
        lock (_fsm) {
            _fsm.Step(from, message);
        }
        return Task.CompletedTask;
    }

    public Task HandleVoteResponseAsync(ulong from, VoteResponse message) {
        lock (_fsm) {
            _fsm.Step(from, message);
        }
        return Task.CompletedTask;
    }

    public Task HandleAppendRequestAsync(ulong from, AppendRequest message) {
        lock (_fsm) {
            _fsm.Step(from, message);
        }
        return Task.CompletedTask;
    }

    public Task HandleAppendResponseAsync(ulong from, AppendResponse message) {
        lock (_fsm) {
            _fsm.Step(from, message);
        }
        return Task.CompletedTask;
    }

    public Task HandleReadQuorumRequestAsync(ulong from, ReadQuorumRequest message) {
        lock (_fsm) {
            _fsm.Step(from, message);
        }
        return Task.CompletedTask;
    }

    public Task HandleReadQuorumResponseAsync(ulong from, ReadQuorumResponse message) {
        lock (_fsm) {
            _fsm.Step(from, message);
        }
        return Task.CompletedTask;
    }

    public Task HandleTimeoutNowAsync(ulong from, TimeoutNowRequest message) {
        lock (_fsm) {
            _fsm.Step(from, message);
        }
        return Task.CompletedTask;
    }

    public Task<PingResponse> HandlePingRequestAsync(ulong from, PingRequest message) {
        return Task.FromResult(new PingResponse());
    }

    public async Task<SnapshotResponse> HandleInstallSnapshotRequestAsync(ulong from, InstallSnapshotRequest message) {
        // tell the state machine to transfer snapshot
        _stateMachine.TransferSnapshot(from, message.Snp);

        var response = new SnapshotResponse { Success = false };

        // step the fsm
        TaskCompletionSource<SnapshotResponse>? promise = null;
        lock (_fsm)
        lock (_snapshotResponsePromises) {
            if (!_snapshotResponsePromises.ContainsKey(from)) {
                _fsm.Step(from, message);
                promise = new TaskCompletionSource<SnapshotResponse>();
                _snapshotResponsePromises.Add(from, promise);
            } else {
                response.CurrentTerm = _fsm.CurrentTerm;
            }
        }

        // return the response
        if (promise == null) {
            return response;
        }

        // retrieve the response
        return await promise.Task;
    }

    public async Task ReadBarrier(object o) {
        throw new NotImplementedException();
    }
}
