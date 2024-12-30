using System.Net;
using Microsoft.Extensions.Logging;
using RaftNET.Services;
using RaftNET.StateMachines;

namespace RaftNET.Tests;

public class RaftTestBase {
    protected ILoggerFactory _loggerFactory;
    protected ILogger<RaftServerTest> _logger;
}

public class RaftServerTest : RaftTestBase {
    private RaftServer _server;
    private AddressBook _addressBook;
    private readonly int _port = 15000;
    private readonly ulong _myId = 1;

    [SetUp]
    public void Setup() {
        _loggerFactory = LoggerFactory.Instance;
        _logger = _loggerFactory.CreateLogger<RaftServerTest>();

        _addressBook = new AddressBook();
        _addressBook.Add(_myId, $"http://127.0.0.1:{_port}");
        var tmpDir = Directory.CreateTempSubdirectory();
        _server = new RaftServer(new RaftService.Config(
            MyId: _myId,
            DataDir: tmpDir.FullName,
            LoggerFactory: _loggerFactory,
            StateMachine: new EmptyStateMachine(),
            AddressBook: _addressBook,
            ListenAddress: IPAddress.Loopback,
            Port: _port
        ));
        _server.Start();    
    }

    [TearDown]
    public void TearDown() {
        _server.Stop();
        _loggerFactory.Dispose();
    }

    [Test]
    public async Task TestRpcServerBasic() {
        var client = new RaftClient(2, $"http://127.0.0.1:{_port}");
        await client.Ping();
    }

    [Test]
    public void TestSingleServerIsLeader() {
        Assert.That(_server.IsLeader, Is.True);
    }
}