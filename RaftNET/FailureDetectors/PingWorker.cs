using Grpc.Core;
using Microsoft.Extensions.Logging;
using RaftNET.Concurrent;
using RaftNET.Services;

namespace RaftNET.FailureDetectors;

public class PingWorker : IPingWorker {
    private readonly ulong _myId;
    private readonly ulong _serverId;
    private readonly AddressBook _addressBook;
    private readonly CancellationTokenSource _cts = new();
    private readonly SemaphoreSlim _waitExit = new(1, 1);
    private readonly Lock _lastAliveLock = new();
    private readonly TimeSpan _timeout;
    private readonly ILogger<PingWorker> _logger;
    private readonly IListener _listener;
    private readonly IClock _clock;
    private readonly TimeSpan _interval;
    private string? _address;
    private DateTime _lastAlive;
    private Task? _pingTask;

    public PingWorker(
        ulong myId, ulong serverId, AddressBook addressBook,
        TimeSpan interval, TimeSpan timeout,
        ILogger<PingWorker> logger, IListener listener, IClock clock
    ) {
        _myId = myId;
        _serverId = serverId;
        _addressBook = addressBook;
        _timeout = timeout;
        _logger = logger;
        _lastAlive = DateTime.Now;
        _listener = listener;
        _clock = clock;
        _interval = interval;
        UpdateAddress();
    }

    public bool IsAlive => _clock.Now < DeadLine;

    private DateTime DeadLine {
        get {
            lock (_lastAliveLock) {
                return _lastAlive + _timeout;
            }
        }
    }

    public void Start() {
        var cancellationToken = _cts.Token;
        _pingTask = Task.Run(() => { Work(cancellationToken); }, cancellationToken);
    }

    public void Stop() {
        _cts.Cancel();

        if (_pingTask != null) {
            _pingTask.Wait();
        }
    }

    public void UpdateAddress() {
        _address = _addressBook.Find(_serverId);
    }

    public void Ping(CancellationToken cancellationToken, IRaftClient client) {
        var ok = true;

        try {
            client.Ping(DeadLine).Wait(cancellationToken);
        }
        catch (RpcException ex) {
            _logger.LogTrace("PingWorker({}) ping to {} failed, ex: {}", _myId, _serverId, ex);
            ok = false;
        }

        lock (_lastAliveLock) {
            var now = _clock.Now;

            if (ok) {
                _lastAlive = now;
            }

            if (now - _lastAlive >= _timeout) {
                _listener.MarkDead(_serverId);
            } else {
                _listener.MarkAlive(_serverId);
            }
        }
    }

    public virtual IRaftClient? GetClient(ulong myId, string? address) {
        if (address != null) {
            return new RaftClient(myId, address);
        }

        return null;
    }

    private void Work(CancellationToken cancellationToken) {
        using var guard = new SemaphoreGuard(_waitExit);
        IRaftClient? client = null;

        while (!cancellationToken.IsCancellationRequested) {
            client ??= GetClient(_myId, _address);

            if (client == null) {
                _logger.LogWarning("PingWorker({}) can't ping {} and we don't have the address", _myId, _serverId);
                continue;
            }

            Ping(cancellationToken, client);
            _logger.LogInformation("Sleeping for {}", _interval);
            _clock.SleepFor(_interval);
        }
    }
}