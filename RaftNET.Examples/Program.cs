using Serilog;
using Spectre.Console.Cli;

namespace RaftNET.Examples;

class Program {
    static int Main(string[] args) {
        Log.Logger = new LoggerConfiguration().WriteTo.Console().CreateLogger();
        var app = new CommandApp();
        app.Configure(c => {
            c.SetApplicationName("RaftNET.Examples");
            c.AddCommand<RunCommand>("run");
        });
        return app.Run(args);
    }
}
