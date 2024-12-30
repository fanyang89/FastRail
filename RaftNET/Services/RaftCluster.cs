using System.Net;
using Microsoft.Extensions.Logging;
using RaftNET.StateMachines;

namespace RaftNET.Services;

public class RaftCluster {
    private readonly Dictionary<ulong, RaftServer> _servers = new();

    public RaftCluster(ILoggerFactory loggerFactory, ulong n = 3) {
        var addressBook = new AddressBook();
        var members = new Dictionary<ulong, bool>();
        for (ulong i = 1; i <= n; i++) {
            members.Add(i, true);
        }
        for (ulong i = 1; i <= n; i++) {
            var tempDir = Directory.CreateTempSubdirectory("raftnet-data");
            addressBook.Add(i, $"http://127.0.0.1:{15000 + i}");
            _servers.Add(i, new RaftServer(new RaftService.Config(
                i,
                tempDir.FullName,
                loggerFactory,
                new EmptyStateMachine(),
                addressBook,
                IPAddress.Loopback,
                (int)(15000 + i)
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