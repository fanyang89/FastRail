namespace FastRail.Tests;

public class SingleServerPingTest : SingleServerTestBase {
    [Test]
    public void TestSingleServerPing() {
        Thread.Sleep(4000);
        Assert.That(Server.PingCount, Is.GreaterThan(0));
    }
}