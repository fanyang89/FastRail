using FastRail.Protos;

namespace FastRail.Server;

using WatcherHandler = Action<string, WatcherEventType>;

public record Watcher(WatcherHandler Handler, bool IsPersistent);

public class WatcherManager {
    private readonly Dictionary<string, List<Watcher>> _watchers = new();
    private readonly Dictionary<string, List<Watcher>> _recursiveWatchers = new();

    public void Register(string watchPath, WatcherHandler handler, bool isPersistent = false, bool isRecursive = false) {
        if (isRecursive) {
            lock (_recursiveWatchers) {
                if (_recursiveWatchers.TryGetValue(watchPath, out var watchers)) {
                    watchers.Add(new Watcher(handler, isPersistent));
                } else {
                    _recursiveWatchers.Add(watchPath, []);
                }
            }
        } else {
            lock (_watchers) {
                if (_watchers.TryGetValue(watchPath, out var watchers)) {
                    watchers.Add(new Watcher(handler, isPersistent));
                } else {
                    _watchers.Add(watchPath, []);
                }
            }
        }
    }

    public void Trigger(string requestPath, WatcherEventType type) {
        lock (_watchers) {
            foreach (var (path, watchers) in _watchers) {
                if (path != requestPath) { continue; }
                foreach (var watcher in watchers) {
                    watcher.Handler.Invoke(requestPath, type);
                }
                _watchers[path] = watchers.Where(w => w.IsPersistent).ToList();
            }
        }

        lock (_recursiveWatchers) {
            foreach (var (path, watchers) in _recursiveWatchers) {
                if (!requestPath.StartsWith(path)) { continue; }
                foreach (var watcher in watchers) {
                    watcher.Handler.Invoke(requestPath, type);
                }
                _recursiveWatchers[path] = watchers.Where(w => w.IsPersistent).ToList();
            }
        }
    }
}