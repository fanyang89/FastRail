using Microsoft.Extensions.Logging;

namespace FastRail.Tests;

[TestFixture]
public class RailTestBase {
    protected ILoggerFactory LoggerFactory;

    [SetUp]
    public void Setup() {
        Console.SetOut(TestContext.Progress);
        LoggerFactory = FastRail.LoggerFactory.Instance;
    }

    [TearDown]
    public void TearDown() {
        LoggerFactory.Dispose();
    }
}