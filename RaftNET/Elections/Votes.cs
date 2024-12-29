using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace RaftNET.Elections;

public class Votes {
    private readonly ElectionTracker _current;
    private readonly ILogger<Votes> _logger;
    private readonly ElectionTracker? _previous;
    private readonly IDictionary<ulong, ServerAddress> _voters = new Dictionary<ulong, ServerAddress>();

    public Votes(Configuration configuration, ILogger<Votes>? logger = null) {
        _logger = logger ?? new NullLogger<Votes>();
        _current = new ElectionTracker(configuration.Current);

        foreach (var member in configuration.Previous)
            if (member.CanVote) {
                if (!_voters.ContainsKey(member.ServerAddress.ServerId)) {
                    _voters.Add(member.ServerAddress.ServerId, member.ServerAddress);
                }
            }

        foreach (var member in configuration.Current)
            if (member.CanVote) {
                if (!_voters.ContainsKey(member.ServerAddress.ServerId)) {
                    _voters.Add(member.ServerAddress.ServerId, member.ServerAddress);
                }
            }

        if (configuration.IsJoint()) {
            _previous = new ElectionTracker(configuration.Previous);
        }
    }

    public ISet<ServerAddress> Voters => new HashSet<ServerAddress>(_voters.Values);

    public void RegisterVote(ulong from, bool granted) {
        var registered = _current.RegisterVote(from, granted);

        if (_previous != null) {
            _previous.RegisterVote(from, granted);
            registered = true;
        }

        // We can get an outdated vote from a node that is now non-voting member.
        // Such vote should be ignored.
        if (!registered) {
            _logger.LogInformation("Got a vote from unregistered server {} during election", from);
        }
    }

    public VoteResult CountVotes() {
        if (_previous != null) {
            var result = _previous.CountVotes();

            if (result != VoteResult.Won) {
                return result;
            }
        }

        return _current.CountVotes();
    }
}