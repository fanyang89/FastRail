using Microsoft.Extensions.Logging;

namespace RaftNET.Tests;

[TestFixture]
public class RaftTestBase {
    protected ILoggerFactory LoggerFactory;

    [SetUp]
    public void Setup() {
        Console.SetOut(TestContext.Progress);
        LoggerFactory = RaftNET.LoggerFactory.Instance;
        AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
    }

    [TearDown]
    public void TearDown() {}
}