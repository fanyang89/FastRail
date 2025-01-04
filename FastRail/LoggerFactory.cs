using Microsoft.Extensions.Logging;

namespace FastRail;

public static class LoggerFactory {
    public static ILoggerFactory Instance { get; } =
        Microsoft.Extensions.Logging.LoggerFactory.Create(Configure());

    private static Action<ILoggingBuilder> Configure() {
        return builder => builder.AddSimpleConsole(
            options => {
                options.IncludeScopes = false;
                options.SingleLine = true;
                options.TimestampFormat = "hh:mm:ss.ffff ";
            }).SetMinimumLevel(LogLevel.Trace);
    }
}