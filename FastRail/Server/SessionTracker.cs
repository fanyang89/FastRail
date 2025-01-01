using Microsoft.Extensions.Logging;

namespace FastRail.Server;

public class SessionTracker : IDisposable, IAsyncDisposable {
    private long _nextSessionId;
    private readonly Timer _timer;
    private readonly Dictionary<long, Session> _sessions = new();
    private SortedSet<long> _expired = new();
    private readonly ILogger<SessionTracker> _logger;
    private readonly Action<long> _onExpired;

    public SessionTracker(TimeSpan tickInterval, Action<long> onExpired, ILogger<SessionTracker> logger) {
        _logger = logger;
        _timer = new Timer(Tick, null, tickInterval, tickInterval);
        _onExpired = onExpired;
    }

    public void Dispose() {
        _timer.Dispose();
    }

    public void Tick(object? state) {
        var now = DateTime.Now;

        lock (_sessions)
        lock (_expired) {
            foreach (var (id, session) in _sessions) {
                if (session.LastLive - now >= session.Timeout) {
                    _logger.LogInformation("Session 0x{:x} expired", id);
                    _expired.Add(id);
                }
            }

            foreach (var id in _expired) {
                _sessions.Remove(id);
            }
        }

        lock (_expired) {
            foreach (var id in _expired) {
                _onExpired(id);
            }

            _expired.Clear();
        }
    }

    public ISet<long> GetExpired() {
        lock (_expired) {
            var expired = _expired;
            _expired = [];
            return expired;
        }
    }

    // create new session, returns session id
    public long Add(TimeSpan timeout) {
        var sessionId = NextSessionId();
        var session = new Session {
            SessionID = sessionId,
            Timeout = timeout,
            LastLive = DateTime.Now
        };

        lock (_sessions) {
            _sessions.Add(sessionId, session);
        }

        return sessionId;
    }

    // touch session
    public void Touch(long sessionId) {
        lock (_sessions) {
            _sessions[sessionId].LastLive = DateTime.Now;
        }
    }

    public long NextSessionId() {
        return Interlocked.Increment(ref _nextSessionId);
    }

    public async ValueTask DisposeAsync() {
        await _timer.DisposeAsync();
    }

    public void Remove(long sessionId) {
        lock (_sessions) {
            // expire at next tick
            _sessions[sessionId].LastLive = new DateTime();
        }
    }
}