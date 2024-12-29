using System.Collections.Concurrent;
using System.Diagnostics;
using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace RaftNET.Services;

public partial class RaftService : Raft.RaftBase, IDisposable {
    private readonly FSM _fsm;
    private readonly Notifier _fsmEventNotify = new();
    private readonly ILogger<RaftService> _logger;
    private readonly IStateMachine _stateMachine;
    private readonly IPersistence _persistence;
    private readonly Timer _ticker;
    private readonly CancellationTokenSource _cancellationTokenSource = new();
    private readonly Task _applyTask;
    private readonly Task _ioTask;
    private readonly BlockingCollection<ApplyMessage> _applyMessages = new();
    private readonly ConnectionManager _connectionManager;

    private ulong _appliedIdx;
    private ulong _snapshotDescIdx;

    public RaftService(Config config) {
        var loggerFactory = config.LoggerFactory;
        _logger = loggerFactory.CreateLogger<RaftService>();
        _stateMachine = config.StateMachine;
        _connectionManager = new ConnectionManager(
            config.MyId, config.AddressBook, loggerFactory.CreateLogger<ConnectionManager>());
        _persistence = new RocksPersistence(config.DataDir);

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
        var fd = new TrivialFailureDetector();
        var fsmConfig = new FSMConfig {
            EnablePreVote = config.EnablePreVote,
            MaxLogSize = config.MaxLogSize,
            AppendRequestThreshold = config.AppendRequestThreshold,
        };
        _fsm = new FSM(
            config.MyId, term, votedFor, log, commitedIdx, fd, fsmConfig, _fsmEventNotify,
            config.LoggerFactory.CreateLogger<FSM>());
        if (snapshot is { Id: > 0 }) {
            _stateMachine.LoadSnapshot(snapshot.Id);
            _snapshotDescIdx = snapshot.Idx;
            _appliedIdx = snapshot.Idx;
        }
        _ticker = new Timer(Tick, null, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(100));

        var token = _cancellationTokenSource.Token;
        _applyTask = Task.Run(DoApply(token), token);
        _ioTask = Task.Run(DoIO(token, 0), token);
    }

    public void Dispose() {
        _ioTask.Wait();
        _applyMessages.Add(new ApplyMessage());
        _applyTask.Wait();
        _ticker.Dispose();
    }

    public async ValueTask DisposeAsync() {
        await _ioTask;
        await _applyTask;
        await _ticker.DisposeAsync();
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
                FSMOutput? batch = null;
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

    private async Task ProcessFSMOutput(ulong lastStable, FSMOutput batch) {
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

    public override Task<Void> Vote(VoteRequest request, ServerCallContext context) {
        var from = GetFromServerId(context.RequestHeaders);
        lock (_fsm) {
            _fsm.Step(from, request);
        }
        return Task.FromResult(new Void());
    }

    public override Task<Void> RespondVote(VoteResponse request, ServerCallContext context) {
        var from = GetFromServerId(context.RequestHeaders);
        lock (_fsm) {
            _fsm.Step(from, request);
        }
        return Task.FromResult(new Void());
    }

    public override Task<Void> Append(AppendRequest request, ServerCallContext context) {
        var from = GetFromServerId(context.RequestHeaders);
        lock (_fsm) {
            _fsm.Step(from, request);
        }
        return Task.FromResult(new Void());
    }

    public override Task<Void> RespondAppend(AppendResponse request, ServerCallContext context) {
        var from = GetFromServerId(context.RequestHeaders);
        lock (_fsm) {
            _fsm.Step(from, request);
        }
        return Task.FromResult(new Void());
    }

    public override Task<Void> SendSnapshot(InstallSnapshot request, ServerCallContext context) {
        var from = GetFromServerId(context.RequestHeaders);
        lock (_fsm) {
            _fsm.Step(from, request);
        }
        return Task.FromResult(new Void());
    }

    public override Task<Void> RespondSendSnapshot(SnapshotResponse request, ServerCallContext context) {
        var from = GetFromServerId(context.RequestHeaders);
        lock (_fsm) {
            _fsm.Step(from, request);
        }
        return Task.FromResult(new Void());
    }

    public override Task<Void> TimeoutNow(TimeoutNowRequest request, ServerCallContext context) {
        var from = GetFromServerId(context.RequestHeaders);
        lock (_fsm) {
            _fsm.Step(from, request);
        }
        return Task.FromResult(new Void());
    }

    public readonly static string KeyFromId = "raftnet-from-id";

    private ulong GetFromServerId(Metadata metadata) {
        Debug.Assert(metadata.Any(x => x.Key == KeyFromId));
        var entry = metadata.First(x => x.Key == KeyFromId);
        return Convert.ToUInt64(entry.Value);
    }
}