using System.Net;
using RaftNET.FailureDetectors;
using RaftNET.Persistence;
using RaftNET.StateMachines;

namespace RaftNET.Services;

public class RaftCluster {
    private readonly Dictionary<ulong, RaftServer> _servers = new();

    public RaftCluster(ulong serverCount) {
        var addressBook = new AddressBook();

        for (ulong i = 1; i <= serverCount; i++) {
            addressBook.Add(i, $"http://127.0.0.1:{15000 + i}");
        }

        for (ulong i = 1; i <= serverCount; i++) {
            var tempDir = Directory.CreateTempSubdirectory("raftnet-data");
            var rpc = new ConnectionManager(i, addressBook);
            var sm = new EmptyStateMachine();
            var persistence = new RocksPersistence(tempDir.FullName);
            var options = new RaftServiceOptions();
            var clock = new SystemClock();
            var fd = new RpcFailureDetector(i, addressBook,
                TimeSpan.FromMilliseconds(options.PingInterval),
                TimeSpan.FromMilliseconds(options.PingTimeout),
                clock);
            var service = new RaftService(i, rpc, sm, persistence, fd, addressBook, new RaftServiceOptions());
            var server = new RaftServer(service, IPAddress.Loopback, 15000 + (int)i);
            _servers.Add(i, server);
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

    public Role[] Roles() {
        var roles = new List<Role>();

        foreach (var (id, server) in _servers) {
            roles.Add(server.Role);
        }

        return roles.ToArray();
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
}
