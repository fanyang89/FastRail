namespace RaftNET.FailureDetectors;

public class DiscreteFailureDetector : IFailureDetector {
    private readonly SortedSet<ulong> _dead = [];
    private bool _isAlive = true;

    public bool IsAlive(ulong id) {
        return _isAlive && !_dead.Contains(id);
    }

    public void MarkAlive(ulong id) {
        _dead.Remove(id);
    }

    public void MarkAllAlive() {
        _isAlive = true;
    }

    public void MarkAllDead() {
        _isAlive = false;
    }

    public void MarkDead(ulong id) {
        _dead.Add(id);
    }
}
