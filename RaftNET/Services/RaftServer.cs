using System.Runtime.InteropServices;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace RaftNET.Services;

public class RaftServer {
    private Task? _runTask;
    private readonly WebApplication _app;
    private readonly RaftService _raftService;

    public RaftServer(RaftService.Config config) {
        _raftService = new RaftService(config);
        var builder = WebApplication.CreateBuilder([]);
        builder.Logging.ClearProviders();
        builder.Logging.AddSimpleConsole(LoggerFactory.ConfigureAspNet());
        builder.WebHost.ConfigureKestrel((_, serverOptions) => {
            serverOptions.Listen(config.ListenAddress, config.Port, options => {
                options.Protocols = HttpProtocols.Http2;
            });
        });
        builder.Services.AddHostedService<RaftService>(_ => _raftService);
        builder.Services.AddSingleton<RaftService>(provider =>
            provider.GetServices<IHostedService>().OfType<RaftService>().First());
        builder.Services.AddGrpc();
        _app = builder.Build();
        _app.MapGrpcService<RaftService>();
    }

    public void Start() {
        _runTask = _app.RunAsync();
    }

    public void Stop() {
        _app.Lifetime.StopApplication();
    }

    public bool IsLeader => _raftService.AcquireFSMLock(fsm => fsm.IsLeader);

    public void AddEntry(byte[] buffer) {
        _raftService.AcquireFSMLock(fsm => fsm.AddEntry(buffer));
    }

    public void AddEntry(Configuration configuration) {
        _raftService.AcquireFSMLock(fsm => fsm.AddEntry(configuration));
    }
}