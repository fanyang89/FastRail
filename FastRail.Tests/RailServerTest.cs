using System.Net;
using FastRail.Server;
using Microsoft.Extensions.Logging;
using RaftNET.Services;

namespace FastRail.Tests;

[TestFixture]
[TestOf(typeof(RailServer))]
public class RailServerTest : RailTestBase {
    private ILogger<RailServerTest> _logger;

    [SetUp]
    public new void Setup() {
        _logger = LoggerFactory.CreateLogger<RailServerTest>();
    }

    [Test]
    public void TestRailServerCanStartAndStop() {
        var tmpDir = Directory.CreateTempSubdirectory();
        var config = new RailServer.Config {
            DataDir = tmpDir.FullName,
            EndPoint = new IPEndPoint(IPAddress.Loopback, 15000)
        };
        var server = new RailServer(config, LoggerFactory);
        server.Start();

        ulong myId = 1;
        var raftPort = 15000 + (int)myId;
        var addressBook = new AddressBook();
        addressBook.Add(myId, raftPort);
        var tmpDir2 = Directory.CreateTempSubdirectory();
        var raftConfig = new RaftService.Config {
            MyId = myId,
            DataDir = tmpDir2.FullName,
            LoggerFactory = LoggerFactory,
            StateMachine = server,
            AddressBook = addressBook,
            Listen = new IPEndPoint(IPAddress.Loopback, raftPort)
        };
        var raftServer = new RaftServer(raftConfig);
        server.SetRaftServer(raftServer);
        raftServer.Start();

        raftServer.Stop();
        server.Stop();
    }
}