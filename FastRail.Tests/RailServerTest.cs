using System.Net;
using FastRail.Server;
using Microsoft.Extensions.Logging;

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
        var config = new RailServer.Config();
        var server = new RailServer(
            new IPEndPoint(IPAddress.Loopback, 15000), config,
            LoggerFactory.CreateLogger<RailServer>()
        );
        server.Start();
        server.Stop();
    }
}