using Microsoft.Extensions.Logging;
using org.apache.zookeeper;

namespace FastRail.Tests;

public class LogWatcher(ILogger<LogWatcher> logger) : Watcher {
    public override Task process(WatchedEvent @event) {
        logger.LogInformation("Process event={}", @event);
        return Task.CompletedTask;
    }
}