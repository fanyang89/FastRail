using org.apache.zookeeper;
using Serilog;

namespace FastRail.Tests;

public class LogWatcher() : Watcher {
    public override Task process(WatchedEvent @event) {
        Log.Information("Process event={}", @event);
        return Task.CompletedTask;
    }
}
