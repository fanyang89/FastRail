using Grpc.Core;
using Microsoft.Extensions.Logging;
using RaftNET.Concurrent;
using RaftNET.Services;

namespace RaftNET;

class PingWorker : IDisposable {
    private readonly ulong _myId;
    private readonly ulong _serverId;
    private readonly AddressBook _addressBook;
    private readonly CancellationTokenSource _cts = new();
    private string? _address;
    private readonly Timer _timer;
    private readonly SemaphoreSlim _waitExit = new(1, 1);
    private DateTime _lastAlive;
    private readonly Lock _lastAliveLock = new();
    private readonly TimeSpan _timeout;
    private readonly ILogger<PingWorker> _logger;
    private readonly IListener _listener;
    private readonly IClock _clock;

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

        UpdateAddress();
        _timer = new Timer(Work, _cts.Token, interval, interval);
    }

    public virtual RaftClient? GetClient() {
        if (_address != null) {
            return new RaftClient(_myId, _address);
        }
        return null;
    }

    private void Work(object? state) {
        var cancellationToken = state as CancellationToken?;
        if (cancellationToken == null) {
            throw new ArgumentException(nameof(state));
        }
        using var guard = new SemaphoreGuard(_waitExit);
        RaftClient? client = null;
        while (!cancellationToken.Value.IsCancellationRequested) {
            client ??= GetClient();
            if (client == null) {
                _logger.LogWarning("PingWorker({}) can't ping {} and we don't have the address", _myId, _serverId);
                continue;
            }

            var ok = true;
            try {
                client.Ping(DeadLine).Wait();
            } catch (RpcException ex) {
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
    }

    public bool IsAlive => _clock.Now < DeadLine;

    private DateTime DeadLine {
        get {
            lock (_lastAliveLock) {
                return _lastAlive + _timeout;
            }
        }
    }

    public void UpdateAddress() {
        _address = _addressBook.Find(_serverId);
    }

    public void Dispose() {
        _cts.Cancel();
        _waitExit.Wait();
        _cts.Dispose();
        _timer.Dispose();
    }
}