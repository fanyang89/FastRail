﻿using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;

namespace RaftNET;

public static class LoggerFactory {
    public static ILoggerFactory Instance { get; } =
        Microsoft.Extensions.Logging.LoggerFactory.Create(Configure());

    public static Action<ILoggingBuilder> Configure() {
        return builder => builder.AddSimpleConsole(
            options => {
                options.IncludeScopes = true;
                options.SingleLine = true;
                options.TimestampFormat = "hh:mm:ss ";
            });
    }

    public static Action<SimpleConsoleFormatterOptions> ConfigureAspNet() {
        return options => {
            options.IncludeScopes = true;
            options.SingleLine = true;
            options.TimestampFormat = "hh:mm:ss ";
        };
    }
}