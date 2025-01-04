using FastRail.Server;
using org.apache.zookeeper;
using org.apache.zookeeper.data;

namespace FastRail.Tests;

public class SingleServerCreateTest : SingleServerTestBase {
    [Test]
    public async Task TestSingleServerCreate() {
        const string path = "/test-node1";
        const string expected = "test-value";

        var realPath = await _client.createAsync(path, expected.ToBytes(), [ACLs.WorldAnyone],
            CreateMode.PERSISTENT);
        Assert.That(realPath, Is.EqualTo(path));

        var stat = await _client.existsAsync(path);
        Assert.That(stat, Is.Not.Null);
    }
}