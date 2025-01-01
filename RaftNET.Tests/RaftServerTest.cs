using System.Net;
using Microsoft.Extensions.Logging;
using RaftNET.Services;
using RaftNET.StateMachines;

namespace RaftNET.Tests;

public class RaftServerTest : RaftTestBase, IStateMachine {
    private ILogger<RaftServerTest> _logger;
    private RaftServer _server;
    private AddressBook _addressBook;
    private string _listenAddress;

    private readonly int _port = 15000;
    private readonly ulong _myId = 1;

    [SetUp]
    public new void Setup() {
        _logger = LoggerFactory.CreateLogger<RaftServerTest>();
        _listenAddress = $"https://127.0.0.1:{_port}";
        _addressBook = new AddressBook();
        _addressBook.Add(_myId, _listenAddress);
        var tmpDir = Directory.CreateTempSubdirectory();
        _server = new RaftServer(new RaftService.Config {
            MyId = _myId,
            DataDir = tmpDir.FullName,
            LoggerFactory = LoggerFactory,
            StateMachine = this,
            AddressBook = _addressBook,
            Listen = new IPEndPoint(IPAddress.Loopback, _port)
        });
        _server.Start();
        _logger.LogInformation("Raft server started at {}", _listenAddress);
    }

    [TearDown]
    public new void TearDown() {
        _server.Stop();
    }

    [Test]
    public void TestSingleServerCanAppend() {
        Assert.That(_server.IsLeader, Is.True);
        _logger.LogInformation("Adding entry");
        _server.AddEntry("Hello World");
        _logger.LogInformation("Entry added");
    }

    [Test]
    public async Task TestRpcServerBasic() {
        var client = new RaftClient(2, _listenAddress);
        await client.Ping();
    }

    [Test]
    public void TestSingleServerIsLeader() {
        Assert.That(_server.IsLeader, Is.True);
    }

    public void Apply(List<Command> commands) {
        foreach (var command in commands) {
            _logger.LogInformation("Applying command: {command}", command);
        }
    }

    public ulong TakeSnapshot() {
        return 0;
    }

    public void DropSnapshot(ulong snapshot) {}

    public void LoadSnapshot(ulong snapshot) {}

    public void OnEvent(Event e) {}
}