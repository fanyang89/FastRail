using Microsoft.Extensions.Logging.Abstractions;
using RaftNET.Elections;

namespace RaftNET.Tests;

public class VotesTest {
    private const ulong Id1 = 1;
    private const ulong Id2 = 2;
    private const ulong Id3 = 3;
    private const ulong Id4 = 4;
    private const ulong Id5 = 5;

    [Test]
    public void TestVotesBasics() {
        var votes = new Votes(Messages.ConfigFromIds(Id1));
        Assert.That(votes.CountVotes(), Is.EqualTo(VoteResult.Unknown));
        Assert.That(votes.Voters, Has.Count.EqualTo(1));

        // Try a vote from an unknown server, it should be ignored.
        votes.RegisterVote(0, true);
        votes.RegisterVote(Id1, false);

        // Quorum votes against the decision
        Assert.That(votes.CountVotes(), Is.EqualTo(VoteResult.Lost));

        // Another vote from the same server is ignored
        votes.RegisterVote(Id1, true);
        votes.RegisterVote(Id1, true);
        Assert.That(votes.CountVotes(), Is.EqualTo(VoteResult.Lost));
    }

    [Test]
    public void TestVotesJointConfiguration1() {
        var votes = new Votes(Messages.ConfigFromIds([Id1], [Id2, Id3]));
        Assert.That(votes.Voters.Count, Is.EqualTo(3));
        Assert.That(votes.CountVotes(), Is.EqualTo(VoteResult.Unknown));
        votes.RegisterVote(Id2, true);
        votes.RegisterVote(Id3, true);
        Assert.That(votes.CountVotes(), Is.EqualTo(VoteResult.Unknown));
        votes.RegisterVote(Id1, false);
        Assert.That(votes.CountVotes(), Is.EqualTo(VoteResult.Lost));
    }

    [Test]
    public void TestVotesJointConfiguration2() {
        var votes = new Votes(Messages.ConfigFromIds([Id1], [Id2, Id3]));
        votes.RegisterVote(Id2, true);
        votes.RegisterVote(Id3, true);
        votes.RegisterVote(Id1, true);
        Assert.That(votes.CountVotes(), Is.EqualTo(VoteResult.Won));
    }

    [Test]
    public void TestVotesJointConfiguration3() {
        var votes = new Votes(Messages.ConfigFromIds([Id1, Id2, Id3], [Id1]));
        Assert.That(votes.Voters.Count, Is.EqualTo(3));
        votes.RegisterVote(Id1, true);
        Assert.That(votes.CountVotes(), Is.EqualTo(VoteResult.Unknown));
        // This gives us a majority in both new and old configurations
        votes.RegisterVote(Id2, true);
        Assert.That(votes.CountVotes(), Is.EqualTo(VoteResult.Won));
    }

    [Test]
    public void TestVotingFourNodes() {
        // Basic voting test for 4 nodes
        var votes = new Votes(Messages.ConfigFromIds(Id1, Id2, Id3, Id4));
        votes.RegisterVote(Id1, true);
        votes.RegisterVote(Id2, true);
        Assert.That(votes.CountVotes(), Is.EqualTo(VoteResult.Unknown));
        votes.RegisterVote(Id3, false);
        Assert.That(votes.CountVotes(), Is.EqualTo(VoteResult.Unknown));
        votes.RegisterVote(Id4, false);
        Assert.That(votes.CountVotes(), Is.EqualTo(VoteResult.Lost));
    }

    [Test]
    public void TestVotingFiveNodes() {
        // Basic voting test for 5 nodes
        var votes = new Votes(Messages.ConfigFromIds([Id1, Id2, Id3, Id4, Id5], [Id1, Id2, Id3]));
        votes.RegisterVote(Id1, false);
        votes.RegisterVote(Id2, false);
        Assert.That(votes.CountVotes(), Is.EqualTo(VoteResult.Lost));
        votes.RegisterVote(Id3, true);
        votes.RegisterVote(Id4, true);
        votes.RegisterVote(Id5, true);
        Assert.That(votes.CountVotes(), Is.EqualTo(VoteResult.Lost));
    }

    [Test]
    public void TestVoterWithNonVoter1() {
        // Basic voting test with tree Voters and one no-voter
        var votes = new Votes(
            new Configuration {
                Current = {
                    Messages.CreateConfigMember(Id1),
                    Messages.CreateConfigMember(Id2),
                    Messages.CreateConfigMember(Id3),
                    Messages.CreateConfigMember(Id4, false)
                }
            }
        );
        votes.RegisterVote(Id1, true);
        votes.RegisterVote(Id2, true);
        Assert.That(votes.CountVotes(), Is.EqualTo(VoteResult.Won));
    }

    [Test]
    public void TestNonVotingVotesIgnored() {
        // Joint configuration with non-voting members
        var votes = new Votes(
            new Configuration {
                Current = { Messages.CreateConfigMember(Id1) },
                Previous = {
                    Messages.CreateConfigMember(Id2), Messages.CreateConfigMember(Id3), Messages.CreateConfigMember(Id4, false)
                }
            }
        );

        Assert.That(votes.Voters.Count, Is.EqualTo(3));
        Assert.That(votes.CountVotes(), Is.EqualTo(VoteResult.Unknown));
        votes.RegisterVote(Id2, true);
        votes.RegisterVote(Id3, true);
        votes.RegisterVote(Id4, true);
        Assert.That(votes.CountVotes(), Is.EqualTo(VoteResult.Unknown));
        votes.RegisterVote(Id1, true);
        Assert.That(votes.CountVotes(), Is.EqualTo(VoteResult.Won));
    }

    [Test]
    public void TestSameNodeVotingAndNonVoting() {
        // Same node is voting in one config and non-voting in another
        var votes = new Votes(
            new Configuration {
                Current = { Messages.CreateConfigMember(Id1), Messages.CreateConfigMember(Id4) },
                Previous = {
                    Messages.CreateConfigMember(Id2), Messages.CreateConfigMember(Id3), Messages.CreateConfigMember(Id4, false)
                }
            }
        );

        votes.RegisterVote(Id2, true);
        votes.RegisterVote(Id1, true);
        votes.RegisterVote(Id4, true);
        Assert.That(votes.CountVotes(), Is.EqualTo(VoteResult.Unknown));
        votes.RegisterVote(Id3, true);
        Assert.That(votes.CountVotes(), Is.EqualTo(VoteResult.Won));
    }
}
