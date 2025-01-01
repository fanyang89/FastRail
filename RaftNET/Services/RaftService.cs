using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OneOf;
using RaftNET.Exceptions;
using RaftNET.FailureDetectors;
using RaftNET.Persistence;
using RaftNET.Records;
using RaftNET.StateMachines;

namespace RaftNET.Services;

public partial class RaftService : Raft.RaftBase, IHostedService {
    private readonly FSM _fsm;
    private readonly Notifier _fsmEventNotify;
    private readonly ILogger<RaftService> _logger;
    private readonly IStateMachine _stateMachine;
    private readonly IPersistence _persistence;
    private readonly BlockingCollection<ApplyMessage> _applyMessages = new();
    private readonly ConnectionManager _connectionManager;
    private readonly AddressBook _addressBook;
    private readonly ulong _myId;
    private readonly ILoggerFactory _loggerFactory;
    private readonly Config _config;
    private ulong _appliedIdx;
    private ulong _snapshotDescIdx;

    private Timer? _ticker;
    private Task? _applyTask;
    private Task? _ioTask;

    public RaftService(Config config) {
        _config = config;
        _loggerFactory = config.LoggerFactory;
        _stateMachine = config.StateMachine;
        _addressBook = config.AddressBook;
        _myId = config.MyId;
        _logger = _loggerFactory.CreateLogger<RaftService>();
        _connectionManager = new ConnectionManager(_myId, _addressBook, _loggerFactory.CreateLogger<ConnectionManager>());
        _persistence = new RocksPersistence(config.DataDir);
        _fsmEventNotify = new Notifier();

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
            snapshot = new SnapshotDescriptor {
                Config = Messages.ConfigFromIds(members)
            };
        }

        var logEntries = _persistence.LoadLog();
        var log = new Log(snapshot, logEntries);
        var fd = new RpcFailureDetector(config.MyId, _addressBook, new SystemClock(), _loggerFactory,
            TimeSpan.FromMilliseconds(config.PingInterval), TimeSpan.FromMilliseconds(config.PingTimeout));
        var fsmConfig = new FSM.Config(
            config.EnablePreVote,
            config.AppendRequestThreshold,
            config.MaxLogSize
        );
        _fsm = new FSM(
            config.MyId, term, votedFor, log, commitedIdx, fd, fsmConfig, _fsmEventNotify,
            config.LoggerFactory.CreateLogger<FSM>());

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
            } else {
                break;
            }
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

    private async Task SendMessage(ulong messageTo, Message message) {
        await _connectionManager.Send(messageTo, message);
    }

    private void Tick(object? state) {
        lock (_fsm) {
            _fsm.Tick();
        }
    }

    public T AcquireFSMLock<T>(Func<FSM, T> fn) {
        lock (_fsm) {
            return fn(_fsm);
        }
    }

    public void AddEntry(OneOf<byte[], Configuration> command, WaitType waitType) {
        var commandLength = command.Match(
            buffer => buffer.Length,
            configuration => configuration.CalculateSize());

        if (commandLength >= _config.MaxCommandSize) {
            throw new CommandTooLargeException(commandLength, _config.MaxCommandSize);
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

    private record TermIdx(ulong Idx, ulong Term);

    private readonly OrderedDictionary<TermIdx, Notifier> _applyNotifiers = new();
    private readonly OrderedDictionary<TermIdx, Notifier> _commitNotifiers = new();

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
}