namespace FastRail.Tests;

public class SingleServerPingTest : SingleServerTestBase {
    [Test]
    public void TestSingleServerPing() {
        Thread.Sleep(4000);
        Assert.That(_server.PingCount, Is.GreaterThan(0));
    }
}