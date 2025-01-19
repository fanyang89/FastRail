using System.Net;
using Microsoft.Extensions.Logging;
using RaftNET.Services;
using RaftNET.StateMachines;

namespace RaftNET.Tests;

public class RaftServerTest : RaftTestBase, IStateMachine {
    private const int Port = 15000;
    private const ulong MyId = 1;
    private ILogger<RaftServerTest> _logger;
    private RaftServer _server;
    private AddressBook _addressBook;
    private string _listenAddress;

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

    public void TransferSnapshot(ulong from, SnapshotDescriptor snapshot) {}

    public void OnEvent(Event e) {}

    [SetUp]
    public new void Setup() {
        _logger = LoggerFactory.CreateLogger<RaftServerTest>();
        _listenAddress = $"http://127.0.0.1:{Port}";
        _addressBook = new AddressBook();
        _addressBook.Add(MyId, _listenAddress);
        var tmpDir = Directory.CreateTempSubdirectory();
        _server = new RaftServer(new RaftServiceConfig {
            MyId = MyId,
            DataDir = tmpDir.FullName,
            LoggerFactory = LoggerFactory,
            StateMachine = this,
            AddressBook = _addressBook,
            Listen = new IPEndPoint(IPAddress.Loopback, Port)
        });
        _ = _server.Start();
        _logger.LogInformation("Raft server started at {}", _listenAddress);
    }

    [TearDown]
    public void TearDown() {
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
    public async Task TestRpcServerBasicAsync() {
        const ulong myId = 2;
        var client = new RaftGrpcClient(myId, _listenAddress);
        var cts = new CancellationTokenSource();
        await client.PingAsync(DateTime.Now + TimeSpan.FromSeconds(1), cts.Token);
    }

    [Test]
    public void TestSingleServerIsLeader() {
        Assert.That(_server.IsLeader, Is.True);
    }
}
