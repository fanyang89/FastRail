using Microsoft.Extensions.Logging;

namespace FastRail.Tests;

[TestFixture]
public class TestBase {
    protected ILoggerFactory LoggerFactory;

    [SetUp]
    public void Setup() {
        Console.SetOut(TestContext.Progress);
        LoggerFactory = FastRail.LoggerFactory.Instance;
        AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
    }

    [TearDown]
    public void TearDown() {}
}