using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RaftNET.FailureDetectors;
using RaftNET.Persistence;
using RaftNET.Records;
using RaftNET.StateMachines;

namespace RaftNET.Services;

public partial class RaftService : Raft.RaftBase, IHostedService {
    private readonly FSM _fsm;
    private readonly Notifier _fsmEventNotify = new();
    private readonly ILogger<RaftService> _logger;
    private readonly IStateMachine _stateMachine;
    private readonly IPersistence _persistence;
    private readonly BlockingCollection<ApplyMessage> _applyMessages = new();
    private readonly ConnectionManager _connectionManager;
    private readonly AddressBook _addressBook;
    private readonly ulong _myId;
    private readonly ILoggerFactory _loggerFactory;
    private ulong _appliedIdx;
    private ulong _snapshotDescIdx;

    private Timer? _ticker;
    private Task? _applyTask;
    private Task? _ioTask;

    public RaftService(Config config) {
        _loggerFactory = config.LoggerFactory;
        _stateMachine = config.StateMachine;
        _addressBook = config.AddressBook;
        _myId = config.MyId;
        _logger = _loggerFactory.CreateLogger<RaftService>();
        _connectionManager = new ConnectionManager(_myId, _addressBook, _loggerFactory.CreateLogger<ConnectionManager>());
        _persistence = new RocksPersistence(config.DataDir);

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
        var logEntries = _persistence.LoadLog();
        var log = new Log(snapshot, logEntries);
        var fd = new RpcFailureDetector(config.MyId,
            _addressBook, new SystemClock(), TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1), _loggerFactory);
        var fsmConfig = new FSM.Config(
            EnablePreVote: config.EnablePreVote,
            AppendRequestThreshold: config.AppendRequestThreshold,
            MaxLogSize: config.MaxLogSize
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
    }

    private Action DoApply(CancellationToken cancellationToken) {
        return () => {
            while (!cancellationToken.IsCancellationRequested) {
                var message = _applyMessages.Take();
                if (message.IsExit) {
                    break;
                }
                message.Switch(
                    entries => {
                        var commandEntries = entries.Where(x => x.Command != null).ToList();
                        var commands = commandEntries.Select(x => x.Command).ToList();
                        _stateMachine.Apply(commands);
                        _appliedIdx = commandEntries.Last().Idx;
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
        };
    }

    private Func<Task> DoIO(CancellationToken cancellationToken, ulong stableIdx) {
        return async () => {
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
                    await ProcessFSMOutput(stableIdx, batch);
                }
            }
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
}