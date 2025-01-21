using System.Net;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;

namespace RaftNET.Services;

public class RaftServer {
    private readonly WebApplication _app;
    private readonly RaftService _raftService;

    public RaftServer(RaftService raftService, IPAddress address, int port) {
        _raftService = raftService;
        var raftGrpcService = new RaftGrpcService(_raftService);

        var builder = WebApplication.CreateBuilder([]);
        builder.Services.AddSerilog();
        builder.WebHost.ConfigureKestrel((_, serverOptions) => {
            serverOptions.Listen(address, port, options => { options.Protocols = HttpProtocols.Http2; });
        });
        builder.Services.AddHostedService<RaftGrpcService>(_ => raftGrpcService);
        builder.Services.AddSingleton<RaftGrpcService>(provider =>
            provider.GetServices<IHostedService>().OfType<RaftGrpcService>().First());
        builder.Services.AddGrpc();

        _app = builder.Build();
        _app.MapGrpcService<RaftGrpcService>();
    }

    public bool IsLeader => _raftService.AcquireFSMLock(fsm => fsm.IsLeader);

    public Role Role => _raftService.AcquireFSMLock(fsm => fsm.Role);

    public void AddEntry(byte[] buffer) {
        _raftService.AddEntry(buffer);
    }

    public void AddEntry(string buffer) {
        AddEntry(Encoding.UTF8.GetBytes(buffer));
    }

    public void AddEntry(Configuration configuration) {
        _raftService.AddEntry(configuration);
    }

    public void AddEntryApplied(byte[] buffer) {
        _raftService.AddEntryApplied(buffer);
    }

    public Task Start() {
        return Task.Run(() => _app.RunAsync());
    }

    public void Stop() {
        _app.Lifetime.StopApplication();
    }
}
