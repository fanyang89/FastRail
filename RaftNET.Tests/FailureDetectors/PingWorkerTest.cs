﻿using Microsoft.Extensions.Logging;
using RaftNET.FailureDetectors;
using RaftNET.Services;

namespace RaftNET.Tests.FailureDetectors;

public class PingWorkerTest : RaftTestBase, IListener {
    private const ulong Id1 = 1;
    private const ulong Id2 = 2;
    private readonly Dictionary<ulong, bool> _alive = new();
    private AddressBook _addressBook;
    private ManualClock _clock;
    private MockPingRaftClient _client;

    public void MarkAlive(ulong server) {
        _alive[server] = true;
    }

    public void MarkDead(ulong server) {
        _alive[server] = false;
    }

    [SetUp]
    public new void Setup() {
        _addressBook = new AddressBook();
        _addressBook.Add(Id2, "fake:port");
        _clock = new ManualClock(DateTime.Now);
        _client = new MockPingRaftClient(Id1, LoggerFactory.CreateLogger<MockPingRaftClient>());
    }

    [Test]
    public void TestPingWorkerBasic() {
        var cts = new CancellationTokenSource();
        var worker = new PingWorker(Id1, Id2, _addressBook,
            TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1), LoggerFactory.CreateLogger<PingWorker>(), this, _clock);

        // t=0, ping success will mark the server as live
        worker.Ping(cts.Token, _client);
        Assert.That(_alive[Id2], Is.True);
        // next ping happened at t=1, and made ping timeout 
        _clock.Advance(TimeSpan.FromSeconds(3));
        _client.InjectPingException();
        worker.Ping(cts.Token, _client);
        Assert.That(_alive[Id2], Is.False);
    }
}