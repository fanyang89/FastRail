using System.Net;
using RaftNET.FailureDetectors;
using RaftNET.Persistence;
using RaftNET.Services;
using RaftNET.StateMachines;
using Serilog;

namespace RaftNET.Tests;

public class RaftServerTest : RaftTestBase, IStateMachine {
    private const int Port = 15000;
    private const ulong MyId = 1;
    private RaftServer _server;
    private AddressBook _addressBook;
    private string _listenAddress;

    public void Apply(List<Command> commands) {
        foreach (var command in commands) {
            Log.Information("Applying command: {command}", command);
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
        _listenAddress = $"http://127.0.0.1:{Port}";
        _addressBook = new AddressBook();
        _addressBook.Add(MyId, _listenAddress);

        var tempDir = Directory.CreateTempSubdirectory();
        var rpc = new ConnectionManager(MyId, _addressBook);
        var sm = new EmptyStateMachine();
        var persistence = new RocksPersistence(tempDir.FullName);
        var options = new RaftServiceOptions();
        var clock = new SystemClock();
        var fd = new RpcFailureDetector(MyId, _addressBook,
            TimeSpan.FromMilliseconds(options.PingInterval),
            TimeSpan.FromMilliseconds(options.PingTimeout),
            clock);
        var service = new RaftService(MyId, rpc, sm, persistence, fd, _addressBook, new RaftServiceOptions());
        _server = new RaftServer(service, IPAddress.Loopback, Port);
        _ = _server.Start();
        Log.Information("Raft server started at {listen}", _listenAddress);
    }

    [TearDown]
    public void TearDown() {
        _server.Stop();
    }

    [Test]
    public void TestSingleServerCanAppend() {
        Assert.That(_server.IsLeader, Is.True);
        _server.AddEntry("Hello World");
    }

    [Test]
    public async Task TestRpcServerBasicAsync() {
        const ulong myId = 2;
        var client = new RaftGrpcClient(myId, _listenAddress);
        var cts = new CancellationTokenSource();
        await client.PingAsync(DateTime.UtcNow + TimeSpan.FromSeconds(1), cts.Token);
    }
}
