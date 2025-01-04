using Microsoft.Extensions.Logging;

namespace FastRail.Tests;

[TestFixture]
public class TestBase {
    protected ILoggerFactory LoggerFactory;

    [SetUp]
    public void Setup() {
        Console.SetOut(TestContext.Progress);
        LoggerFactory = FastRail.LoggerFactory.Instance;
    }

    [TearDown]
    public void TearDown() {}

    protected static string CreateTempDirectory() {
        return Directory.CreateTempSubdirectory().FullName;
    }
}