using Spectre.Console.Cli;

namespace RaftNET.Examples;

class Program {
    static int Main(string[] args) {
        var app = new CommandApp();
        app.Configure(c => {
            c.SetApplicationName("RaftNET.Examples");
            c.AddCommand<RunCommand>("run");
        });
        return app.Run(args);
    }
}
