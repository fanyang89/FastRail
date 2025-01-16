using FastRail.Server;
using org.apache.zookeeper;
using org.apache.zookeeper.data;

namespace FastRail.Tests;

public class SingleServerGetSetTest : SingleServerTestBase {
    [Test]
    public async Task TestSingleServerGetSet() {
        const string path = "/test-node2";
        const string expected = "test-value";

        var realPath = await Client.createAsync(path, expected.ToBytes(),
            [new ACL((int)ZooDefs.Perms.ALL, ZooDefs.Ids.ANYONE_ID_UNSAFE)],
            CreateMode.PERSISTENT);
        Assert.That(realPath, Is.EqualTo(path));

        var stat = await Client.existsAsync(path);
        Assert.That(stat, Is.Not.Null);

        var result = await Client.getDataAsync(path);
        Assert.That(result.Data, Is.EqualTo(expected.ToBytes()));
    }
}
