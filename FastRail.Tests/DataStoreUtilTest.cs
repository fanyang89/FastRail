using FastRail.Server;

namespace FastRail.Tests;

public class DataStoreUtilTest {
    [Test]
    public void TestGetParentPath() {
        Assert.Throws<ArgumentException>(() => DataStore.GetParentPath("/"));
        Assert.That(DataStore.GetParentPath("/test-node"), Is.EqualTo("/"));
        Assert.That(DataStore.GetParentPath("/1/2/3"), Is.EqualTo("/1/2"));
    }
}