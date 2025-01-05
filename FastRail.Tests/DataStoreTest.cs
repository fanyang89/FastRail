using FastRail.Protos;
using FastRail.Server;
using Microsoft.Extensions.Logging;

namespace FastRail.Tests;

public class DataStoreTest : TestBase {
    private DataStore _ds;

    [SetUp]
    public new void Setup() {
        _ds = new DataStore(CreateTempDirectory(),
            TimeSpan.FromMilliseconds(1000),
            LoggerFactory.CreateLogger<DataStore>());
        _ds.Start();
    }

    [TearDown]
    public new void TearDown() {
        _ds.Stop();
        _ds.Dispose();
    }

    [Test]
    public void TestRootNodeExists() {
        var root = _ds.GetNodeStat("/");
        Assert.That(root, Is.Not.Null);
    }

    [Test]
    public void TestCreateNode() {
        var root = _ds.GetNodeStat("/");
        Assert.That(root, Is.Not.Null);
        Assert.That(root.NumChildren, Is.Zero);

        const string node1 = "/test-node1";
        var nodeData1 = "test-node1"u8.ToArray();
        _ds.CreateNode(node1, nodeData1, 1,
            new StatEntry {
                Czxid = 1,
                Mzxid = 1,
                Ctime = Time.CurrentTimeMillis(),
                Mtime = Time.CurrentTimeMillis(),
                Version = 0,
                Cversion = 0,
                Aversion = 0,
                EphemeralOwner = 0,
                Pzxid = 0,
                DataLength = nodeData1.Length,
                NumChildren = 0
            });

        var node = _ds.GetNodeStat(node1);
        Assert.That(node, Is.Not.Null);
        Assert.That(node.NumChildren, Is.EqualTo(0));

        root = _ds.GetNodeStat("/");
        Assert.That(root, Is.Not.Null);
        Assert.That(root.NumChildren, Is.EqualTo(1));
    }
}

public class DataStoreUtilTest {
    [Test]
    public void TestGetParentPath() {
        Assert.Throws<ArgumentException>(() => DataStore.GetParentPath("/"));
        Assert.That(DataStore.GetParentPath("/test-node"), Is.EqualTo("/"));
        Assert.That(DataStore.GetParentPath("/1/2/3"), Is.EqualTo("/1/2"));
    }
}