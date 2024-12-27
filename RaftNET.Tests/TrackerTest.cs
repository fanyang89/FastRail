namespace RaftNET.Tests;

public class TrackerTest {
    private Tracker _tracker;

    [SetUp]
    public void Setup() {
        _tracker = new Tracker();
    }

    private FollowerProgress Find(ulong id) {
        var p = _tracker.Find(id);
        Assert.That(p, Is.Not.Null);
        return p;
    }

    [Test]
    public void TrackerBasic() {
        const ulong id1 = 1;
        const ulong id2 = 2;
        const ulong id3 = 3;
        const ulong id4 = 4;
        const ulong id5 = 5;

        var cfg = Messages.ConfigFromIds(id1);
        _tracker.SetConfiguration(cfg, 1);

        Assert.Multiple(() => {
            Assert.That(_tracker.Find(id1), Is.Not.Null);
            Assert.That(_tracker.Committed(0), Is.EqualTo(0));
        });
        Assert.Multiple(() => {
            Assert.That(_tracker.Find(id1).MatchIdx, Is.EqualTo(0));
            Assert.That(_tracker.Find(id1).NextIdx, Is.EqualTo(1));
        });

        _tracker.Find(id1).Accepted(1);
        Assert.Multiple(() => {
            Assert.That(_tracker.Find(id1).MatchIdx, Is.EqualTo(1));
            Assert.That(_tracker.Find(id1).NextIdx, Is.EqualTo(2));
            Assert.That(_tracker.Committed(0), Is.EqualTo(1));
        });

        _tracker.Find(id1).Accepted(10);
        Assert.Multiple(() => {
            Assert.That(_tracker.Find(id1).MatchIdx, Is.EqualTo(10));
            Assert.That(_tracker.Find(id1).NextIdx, Is.EqualTo(11));
            Assert.That(_tracker.Committed(0), Is.EqualTo(10));
        });

        // Out of order confirmation is OK
        _tracker.Find(id1).Accepted(5);
        Assert.Multiple(() => {
            Assert.That(_tracker.Find(id1).MatchIdx, Is.EqualTo(10));
            Assert.That(_tracker.Find(id1).NextIdx, Is.EqualTo(11));
            Assert.That(_tracker.Committed(5), Is.EqualTo(10));
        });

        // Enter joint configuration {A,B,C}
        cfg.EnterJoint(Messages.CreateConfigMembers(id1, id2, id3));
        _tracker.SetConfiguration(cfg, 1);
        Assert.That(_tracker.Committed(10), Is.EqualTo(10));
        Find(id2).Accepted(11);
        Assert.That(_tracker.Committed(10), Is.EqualTo(10));
        Find(id3).Accepted(12);
        Assert.That(_tracker.Committed(10), Is.EqualTo(10));
        Find(id1).Accepted(13);
        Assert.That(_tracker.Committed(10), Is.EqualTo(12));
        Find(id1).Accepted(14);
        Assert.That(_tracker.Committed(13), Is.EqualTo(13));

        // Leave joint configuration, final configuration is {A,B,C}
        cfg.LeaveJoint();
        _tracker.SetConfiguration(cfg, 1);
        Assert.That(_tracker.Committed(13), Is.EqualTo(13));

        cfg.EnterJoint(Messages.CreateConfigMembers(id3, id4, id5));
        _tracker.SetConfiguration(cfg, 1);
        Assert.That(_tracker.Committed(13), Is.EqualTo(13));
        Find(id1).Accepted(15);
        Assert.That(_tracker.Committed(13), Is.EqualTo(13));
        Find(id5).Accepted(15);
        Assert.That(_tracker.Committed(13), Is.EqualTo(13));
        Find(id3).Accepted(15);
        Assert.That(_tracker.Committed(13), Is.EqualTo(15));
        // This does not advance the joint quorum
        Find(id1).Accepted(16);
        Find(id4).Accepted(17);
        Find(id5).Accepted(18);
        Assert.That(_tracker.Committed(15), Is.EqualTo(15));

        cfg.LeaveJoint();
        _tracker.SetConfiguration(cfg, 1);
        // Leaving joint configuration commits more entries
        Assert.That(_tracker.Committed(15), Is.EqualTo(17));

        cfg.EnterJoint(Messages.CreateConfigMembers(id1));
        cfg.LeaveJoint();
        cfg.EnterJoint(Messages.CreateConfigMembers(id2));
        _tracker.SetConfiguration(cfg, 1);
        // Sic: we're in a weird state. The joint commit index
        // is actually 1, since id2 is at position 1. But in
        // unwinding back the commit index would be weird,
        // so we report back the hint (prev_commit_idx).
        // As soon as the cluster enters joint configuration,
        // and old quorum is insufficient, the leader won't be able to
        // commit new entries until the new members catch up.
        Assert.That(_tracker.Committed(17), Is.EqualTo(17));
        Find(id1).Accepted(18);
        Assert.That(_tracker.Committed(17), Is.EqualTo(17));
        Find(id2).Accepted(19);
        Assert.That(_tracker.Committed(17), Is.EqualTo(18));
        Find(id1).Accepted(20);
        Assert.That(_tracker.Committed(18), Is.EqualTo(19));

        // Check that non-voting member is not counted for the quorum in simple config
        cfg.EnterJoint(new HashSet<ConfigMember> {
            Messages.CreateConfigMember(id1),
            Messages.CreateConfigMember(id2),
            Messages.CreateConfigMember(id3, false),
        });
        cfg.LeaveJoint();
        _tracker.SetConfiguration(cfg, 1);
        Find(id1).Accepted(30);
        Find(id2).Accepted(25);
        Find(id3).Accepted(30);
        Assert.That(_tracker.Committed(0), Is.EqualTo(25));

        // Check that non-voting member is not counted for the quorum in joint config
        cfg.EnterJoint(new HashSet<ConfigMember> {
            Messages.CreateConfigMember(id4),
            Messages.CreateConfigMember(id5),
        });
        _tracker.SetConfiguration(cfg, 1);
        Find(id4).Accepted(30);
        Find(id5).Accepted(30);
        Assert.That(_tracker.Committed(0), Is.EqualTo(25));

        // Check the case where the same node is in both config but different voting rights
        cfg.LeaveJoint();
        cfg.EnterJoint(new HashSet<ConfigMember> {
            Messages.CreateConfigMember(id1),
            Messages.CreateConfigMember(id2),
            Messages.CreateConfigMember(id5, false),
        });
        Assert.That(_tracker.Committed(0), Is.EqualTo(25));
    }
}