using FastRail.Server;
using org.apache.zookeeper;

namespace FastRail.Tests;

public class SingleServerDeleteTest : SingleServerTestBase {
    [Test]
    public async Task TestSingleServerDelete() {
        const string path = "/test-node1";
        const string expected = "test-value";

        await _client.createAsync(path, expected.ToBytes(), [ACLs.WorldAnyone], CreateMode.PERSISTENT);
        var stat = await _client.existsAsync(path);
        Assert.That(stat, Is.Not.Null);

        await _client.deleteAsync(path);
        stat = await _client.existsAsync(path);
        Assert.That(stat, Is.Null);
    }
}