using Microsoft.Extensions.Logging;

namespace FastRail.Tests;

[TestFixture]
public class TestBase {
    [SetUp]
    public void Setup() {
        Console.SetOut(TestContext.Progress);
        LoggerFactory = FastRail.LoggerFactory.Instance;
    }

    [TearDown]
    public void TearDown() {}

    protected ILoggerFactory LoggerFactory;

    protected static string CreateTempDirectory() {
        return Directory.CreateTempSubdirectory().FullName;
    }
}
