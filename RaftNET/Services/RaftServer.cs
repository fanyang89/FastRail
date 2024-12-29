using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RaftNET.StateMachines;

namespace RaftNET.Services;

public class RaftServer {
    private Task? _runTask;
    private readonly WebApplication _app;
    private readonly RaftService _raftService;

    public RaftServer(RaftService.Config config) {
        _raftService = new RaftService(config);
        var builder = WebApplication.CreateBuilder([]);
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

    public bool IsLeader => _raftService.RunFSM(fsm => fsm.IsLeader);
}

public class RaftCluster {
    private readonly Dictionary<ulong, RaftServer> _servers = new();

    public RaftCluster(ILoggerFactory loggerFactory, ulong n = 3) {
        var addressBook = new AddressBook();
        for (ulong i = 1; i <= n; i++) {
            var tempDir = Directory.CreateTempSubdirectory("raftnet-data");
            addressBook.Add(i, $"http://127.0.0.1:{15000 + i}");
            _servers.Add(i, new RaftServer(new RaftService.Config(
                MyId: i,
                DataDir: tempDir.FullName,
                LoggerFactory: loggerFactory,
                StateMachine: new EmptyStateMachine(),
                AddressBook: addressBook,
                ListenAddress: IPAddress.Loopback,
                Port: (int)(15000 + i)
            )));
        }
    }

    public void Start() {
        foreach (var server in _servers.Values) {
            server.Start();
        }
    }

    public void Stop() {
        foreach (var server in _servers.Values) {
            server.Stop();
        }
    }

    public ulong? FindLeader() {
        foreach (var (id, server) in _servers) {
            if (server.IsLeader) {
                return id;
            }
        }
        return null;
    }
}