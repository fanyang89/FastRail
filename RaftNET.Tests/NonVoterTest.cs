using RaftNET.FailureDetectors;
using RaftNET.Replication;

namespace RaftNET.Tests;

public class NonVoterTest : FSMTestBase {
    [Test]
    public void TestLeaderChangeToNonVoter() {
        var cfg = new Configuration {
            Current = {
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = A_ID }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = B_ID }, CanVote = false },
            }
        };
        var log = new RaftLog(new SnapshotDescriptor { Idx = 0, Config = cfg });
        var a = CreateFollower(A_ID, log.Clone());
        var b = CreateFollower(B_ID, log.Clone());
        ElectionTimeout(a);
        Communicate(a, b);
        Assert.That(a.IsLeader, Is.True);
        var newCfg = new Configuration {
            Current = {
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = A_ID }, CanVote = false },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = B_ID }, CanVote = true },
            }
        };
        a.AddEntry(newCfg);
        a.Tick();
        Communicate(a, b);
        Assert.Multiple(() => {
            Assert.That(a.IsFollower, Is.True);
            Assert.That(b.IsLeader, Is.True);
        });
        newCfg = new Configuration {
            Current = {
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = A_ID }, CanVote = false },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = B_ID }, CanVote = false },
            }
        };
        Assert.Throws<ArgumentException>(() => b.AddEntry(newCfg));
        newCfg = new Configuration {
            Current = {
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = B_ID }, CanVote = false },
            }
        };
        Assert.Throws<ArgumentException>(() => b.AddEntry(newCfg));
    }

    [Test]
    public void TestNonVoterCanVote() {
        var fd = new DiscreteFailureDetector();
        var cfg = new Configuration {
            Current = {
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = A_ID }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = B_ID }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = C_ID }, CanVote = false },
            }
        };
        var log = new RaftLog(new SnapshotDescriptor {
            Idx = 0, Config = cfg
        });
        var a = CreateFollower(A_ID, log.Clone(), fd);
        var b = CreateFollower(B_ID, log.Clone(), fd);
        var c = CreateFollower(C_ID, log.Clone(), fd);
        ElectionTimeout(a);
        Communicate(a, b, c);
        Assert.That(a.IsLeader, Is.True);

        var cfgAllVoters = Messages.ConfigFromIds(A_ID, B_ID, C_ID);
        a.AddEntry(cfgAllVoters);
        Communicate(a, b);
        Assert.Multiple(() => {
            Assert.That(a.GetConfiguration().IsJoint(), Is.False);
            Assert.That(a.GetConfiguration().Current.First(x => x.ServerAddress.ServerId == C_ID).CanVote, Is.True);
            Assert.That(a.LogLastIdx, Is.EqualTo(b.LogLastIdx));
        });
        fd.MarkDead(A_ID);
        ElectionTimeout(b);
        ElectionThreshold(c);
        Communicate(b, c);
        Assert.Multiple(() => {
            Assert.That(b.IsLeader, Is.True);
            Assert.That(b.CurrentTerm, Is.EqualTo(c.CurrentTerm));
            Assert.That(b.LogLastIdx, Is.EqualTo(c.LogLastIdx));
        });
    }

    [Test]
    public void TestNonVoterConfigurationChangeInSnapshot() {
        var fd = new DiscreteFailureDetector();
        var cfg = Messages.ConfigFromIds(A_ID, B_ID, C_ID);
        var log = new RaftLog(new SnapshotDescriptor { Idx = 0, Config = cfg });
        var a = CreateFollower(A_ID, log.Clone(), fd);
        var b = CreateFollower(B_ID, log.Clone(), fd);
        var c = CreateFollower(C_ID, log.Clone(), fd);
        ElectionTimeout(a);
        Communicate(a, b, c);
        Assert.That(a.IsLeader, Is.True);

        a.AddEntry(new Void());
        var cfgWithNonVoter = new Configuration {
            Current = {
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = A_ID }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = B_ID }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = C_ID }, CanVote = false },
            }
        };
        a.Tick();
        a.AddEntry(cfgWithNonVoter);
        a.Tick();

        Communicate(a, b);
        Assert.That(a.GetConfiguration().IsJoint, Is.False);
        Assert.That(a.GetConfiguration().Current.First(x => x.ServerAddress.ServerId == C_ID).CanVote, Is.False);
        a.Tick();
        var aSnp = new SnapshotDescriptor {
            Idx = a.LogLastIdx,
            Term = a.LogLastTerm,
            Config = a.GetConfiguration()
        };
        a.ApplySnapshot(aSnp, 0, 0, true);
        a.Tick();
        Communicate(a, b, c);
        Assert.Multiple(() => {
            Assert.That(a.IsLeader, Is.True);
            Assert.That(a.CurrentTerm, Is.EqualTo(c.CurrentTerm));
            Assert.That(a.LogLastIdx, Is.EqualTo(c.LogLastIdx));
        });

        fd.MarkAllDead();
        ElectionTimeout(c);
        Assert.That(c.IsFollower, Is.True);

        fd.MarkAllAlive();
        a.Tick();
        for (int i = 0; i < 100; i++) {
            a.AddEntry(new Void());
        }
        a.AddEntry(cfg);
        a.Tick();

        Communicate(a, b);
        Assert.That(a.GetConfiguration().IsJoint, Is.False);
        Assert.That(a.GetConfiguration().Current.First(x => x.ServerAddress.ServerId == C_ID).CanVote, Is.True);
        a.Tick();
        aSnp = new SnapshotDescriptor {
            Idx = a.LogLastIdx,
            Term = a.LogLastTerm,
            Config = a.GetConfiguration()
        };
        a.ApplySnapshot(aSnp, 0, 0, true);
        a.Tick();
        Communicate(a, b, c);
        Assert.Multiple(() => {
            Assert.That(a.IsLeader, Is.True);
            Assert.That(a.CurrentTerm, Is.EqualTo(c.CurrentTerm));
            Assert.That(a.LogLastIdx, Is.EqualTo(c.LogLastIdx));
        });
        fd.MarkAllDead();
        ElectionTimeout(c);
        Assert.That(c.IsCandidate, Is.True);
        ElectionThreshold(b);
        Communicate(c, b);
        Assert.That(c.IsLeader, Is.True);
    }

    [Test]
    public void TestNonVoterElectionTimeout() {
        var fd = new DiscreteFailureDetector();
        var cfg = new Configuration {
            Current = {
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = A_ID }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = B_ID }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = C_ID }, CanVote = false },
            }
        };
        var log = new RaftLog(new SnapshotDescriptor { Idx = 0, Config = cfg });

        var a = CreateFollower(A_ID, log.Clone(), fd);
        var b = CreateFollower(B_ID, log.Clone(), fd);
        var c = CreateFollower(C_ID, log.Clone(), fd);

        ElectionTimeout(a);
        Communicate(a, b, c);
        Assert.That(a.IsLeader, Is.True);

        fd.MarkAllDead();
        var cTerm = c.CurrentTerm;
        ElectionTimeout(c);
        Assert.Multiple(() => {
            Assert.That(c.IsFollower, Is.True);
            Assert.That(cTerm, Is.EqualTo(c.CurrentTerm));
        });
    }

    [Test]
    public void TestNonVoterGetTimeoutNow() {
        var cfg = new Configuration {
            Current = {
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = A_ID }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = B_ID }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = C_ID }, CanVote = false },
            }
        };
        var log = new RaftLog(new SnapshotDescriptor { Idx = 0, Config = cfg });
        var a = CreateFollower(A_ID, log.Clone());
        var b = CreateFollower(B_ID, log.Clone());
        var c = CreateFollower(C_ID, log.Clone());
        ElectionTimeout(a);
        Communicate(a, b, c);
        Assert.That(a.IsLeader, Is.True);
        c.Step(A_ID, new TimeoutNowRequest {
            CurrentTerm = a.CurrentTerm
        });
        c.Tick();
        var output = c.GetOutput();
        Assert.Multiple(() => {
            Assert.That(c.IsFollower, Is.True);
            Assert.That(output.Messages, Is.Empty);
            Assert.That(output.TermAndVote, Is.Null);
        });
        a.AddEntry(new Void());
        Communicate(a, b, c);
        Assert.Multiple(() => {
            Assert.That(a.LogLastIdx, Is.EqualTo(c.LogLastIdx));
            Assert.That(a.CurrentTerm, Is.EqualTo(c.CurrentTerm));
            Assert.That(a.IsLeader, Is.True);
        });
    }

    [Test]
    public void TestNonVoterStaysPipeline() {
        // Check that a node stays in PIPELINE mode through configuration changes
        var cfg = new Configuration {
            Current = {
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = A_ID }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = B_ID }, CanVote = false }
            }
        };
        var log = new RaftLog(new SnapshotDescriptor { Idx = 0, Config = cfg });
        var a = CreateFollower(A_ID, log.Clone());
        var b = CreateFollower(B_ID, log.Clone());
        ElectionTimeout(a);
        Communicate(a);
        Assert.That(a.IsLeader, Is.True);
        var progress = a.GetProgress(B_ID);
        Assert.That(progress, Is.Not.Null);
        Assert.That(progress.State, Is.EqualTo(FollowerProgressState.Probe));
        a.AddEntry(new Void());
        a.Tick();
        Communicate(a, b);
        progress = a.GetProgress(B_ID);
        Assert.That(progress, Is.Not.Null);
        Assert.That(progress.State, Is.EqualTo(FollowerProgressState.Pipeline));
        var newCfg = Messages.ConfigFromIds(A_ID, B_ID);
        a.AddEntry(newCfg);
        Communicate(a, b);

        Assert.That(a.GetConfiguration().IsJoint, Is.False);
        Assert.That(a.GetConfiguration().Current.First(x => x.ServerAddress.ServerId == B_ID).CanVote, Is.True);

        progress = a.GetProgress(B_ID);
        Assert.That(progress, Is.Not.Null);
        Assert.That(progress.State, Is.EqualTo(FollowerProgressState.Pipeline));
        a.AddEntry(cfg);

        CommunicateUntil(() => {
            if (RollDice()) {
                a.Tick();
                b.Tick();
            }
            return false;
        }, a, b);
        Assert.That(a.GetConfiguration().IsJoint, Is.False);
        Assert.That(a.GetConfiguration().Current.First(x => x.ServerAddress.ServerId == B_ID).CanVote, Is.False);

        progress = a.GetProgress(B_ID);
        Assert.That(progress, Is.Not.Null);
        Assert.That(progress.State, Is.EqualTo(FollowerProgressState.Pipeline));
    }

    [Test]
    public void TestNonVoterVoterLoop() {
        // Test voter-non-voter change in a loop
        var cfg = Messages.ConfigFromIds(A_ID, B_ID, C_ID);
        var cfgWithNonVoter = new Configuration {
            Current = {
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = A_ID }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = B_ID }, CanVote = true },
                new ConfigMember { ServerAddress = new ServerAddress { ServerId = C_ID }, CanVote = false },
            }
        };

        var log = new RaftLog(new SnapshotDescriptor { Idx = 0, Config = cfgWithNonVoter });
        var a = CreateFollower(A_ID, log.Clone());
        var b = CreateFollower(B_ID, log.Clone());
        var c = CreateFollower(C_ID, log.Clone());
        ElectionTimeout(a);
        Communicate(a, b, c);
        Assert.That(a.IsLeader, Is.True);

        for (var i = 0; i < 100; i++) {
            a.AddEntry(i % 2 == 1 ? cfgWithNonVoter : cfg);
            if (RollDice()) {
                a.AddEntry(new Void());
            }
            Communicate(a, b, c);
            if (RollDice()) {
                a.AddEntry(new Void());
                Communicate(a, b, c);
            }
            if (RollDice(1.0f / 1000)) {
                a.RaftLog.ApplySnapshot(Messages.LogSnapshot(a.RaftLog, a.LogLastIdx), 0, 0);
            }
            if (RollDice(1.0f / 100)) {
                b.RaftLog.ApplySnapshot(Messages.LogSnapshot(a.RaftLog, b.LogLastIdx), 0, 0);
            }
            if (RollDice(1.0f / 5000)) {
                c.RaftLog.ApplySnapshot(Messages.LogSnapshot(a.RaftLog, b.LogLastIdx), 0, 0);
            }
        }

        Assert.Multiple(() => {
            Assert.That(a.IsLeader, Is.True);
            Assert.That(a.CurrentTerm, Is.EqualTo(c.CurrentTerm));
            Assert.That(a.LogLastIdx, Is.EqualTo(c.LogLastIdx));
        });
    }
}
