namespace RaftNET.Tests;

public class TrackerTest {
    private Tracker _tracker;

    [SetUp]
    public void Setup() {
        _tracker = new Tracker();
    }

    [Test]
    public void TrackerBasic() {
        ulong id1 = 1;

        var cfg = Messages.ConfigFromIds(id1);
        _tracker.SetConfiguration(cfg, 1);

        Assert.That(_tracker.Find(id1), Is.Not.Null);
        Assert.That(_tracker.Committed(0), Is.EqualTo(0));

        Assert.That(_tracker.Find(id1).MatchIdx, Is.EqualTo(0));
        Assert.That(_tracker.Find(id1).NextIdx, Is.EqualTo(1));

        _tracker.Find(id1).Accepted(1);
        Assert.That(_tracker.Find(id1).MatchIdx, Is.EqualTo(1));
        Assert.That(_tracker.Find(id1).NextIdx, Is.EqualTo(2));
        Assert.That(_tracker.Committed(0), Is.EqualTo(1));
    }
}