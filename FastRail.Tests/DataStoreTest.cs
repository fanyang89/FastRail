using FastRail.Protos;
using FastRail.Server;
using Google.Protobuf;

namespace FastRail.Tests;

public class DataStoreTest : TestBase {
    private DataStore _ds;

    [SetUp]
    public new void Setup() {
        _ds = new DataStore(CreateTempDirectory());
        _ds.Start();
    }

    [TearDown]
    public void TearDown() {
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
        Assert.That(root.Cversion, Is.Zero);

        const string node1 = "/test-node1";
        var nodeData1 = "test-node1"u8.ToArray();
        _ds.CreateNode(123,
            new CreateNodeTransaction {
                Path = node1, Data = ByteString.CopyFrom(nodeData1), Ctime = Time.CurrentTimeMillis(),
            });

        var node = _ds.GetNodeStat(node1);
        Assert.That(node, Is.Not.Null);
        Assert.That(node.NumChildren, Is.EqualTo(0));

        root = _ds.GetNodeStat("/");
        Assert.That(root, Is.Not.Null);
        Assert.That(root.NumChildren, Is.EqualTo(1));
        Assert.That(root.Cversion, Is.EqualTo(1));
        Assert.That(root.Pzxid, Is.EqualTo(123));
    }

    [Test]
    public void TestRemoveNode() {
        var root = _ds.GetNodeStat("/");
        Assert.That(root, Is.Not.Null);
        Assert.That(root.NumChildren, Is.Zero);

        Assert.Throws<ArgumentException>(() => _ds.RemoveNode(1, new DeleteNodeTransaction { Path = "/" }));
        _ds.CreateNode(2, new CreateNodeTransaction {
            Path = "/test-node1",
            Data = ByteString.CopyFrom([]),
            Ctime = Time.CurrentTimeMillis(),
        });

        root = _ds.GetNodeStat("/");
        Assert.That(root, Is.Not.Null);
        Assert.That(root.NumChildren, Is.EqualTo(1));

        _ds.RemoveNode(2, new DeleteNodeTransaction { Path = "/test-node1" });

        root = _ds.GetNodeStat("/");
        Assert.That(root, Is.Not.Null);
        Assert.That(root.NumChildren, Is.EqualTo(0));
    }

    [Test]
    public void TestUpdateNode() {
        const string nodePath = "/test-node1";
        _ds.CreateNode(1, new CreateNodeTransaction {
            Path = nodePath,
            Data = ByteString.CopyFrom("123"u8.ToArray()),
            Ctime = Time.CurrentTimeMillis(),
        });
        Assert.That(_ds.GetNodeData(nodePath), Is.EqualTo("123"u8.ToArray()));

        _ds.UpdateNode(2, new UpdateNodeTransaction {
            Path = nodePath,
            Data = ByteString.CopyFrom("456"u8.ToArray()),
            Mtime = Time.CurrentTimeMillis(),
            Version = 1
        });
        Assert.That(_ds.GetNodeData(nodePath), Is.EqualTo("456"u8.ToArray()));
    }
}
