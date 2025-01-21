using FastRail.Protos;

namespace FastRail.Server;

public record Watcher(Action<string, WatcherEventType> Handler, bool IsPersistent);
