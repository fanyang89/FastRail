namespace RaftNET.FailureDetectors;

public class DiscreteFailureDetector : IFailureDetector {
    private readonly ISet<ulong> _dead = new SortedSet<ulong>();
    private bool _isAlive = true;

    public bool IsAlive(ulong id) {
        return _isAlive && !_dead.Contains(id);
    }

    public void MarkDead(ulong id) {
        _dead.Add(id);
    }

    public void MarkAlive(ulong id) {
        _dead.Remove(id);
    }

    public void MarkAllDead() {
        _isAlive = false;
    }

    public void MarkAllAlive() {
        _isAlive = true;
    }
}