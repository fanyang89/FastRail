using Serilog;

namespace FastRail.Tests;

[TestFixture]
public class TestBase {
    [SetUp]
    public void Setup() {
        Console.SetOut(TestContext.Progress);
        Log.Logger = new LoggerConfiguration().WriteTo.Console().CreateLogger();
    }

    protected static string CreateTempDirectory() {
        return Directory.CreateTempSubdirectory().FullName;
    }
}
