using RaftNET.Replication;

namespace RaftNET;

public class ActivityTracker(Tracker tracker) {
    private int _cur;
    private int _prev;

    public void Invoke(ulong id) {
        _cur += tracker.CurrentVoters.Contains(id) ? 1 : 0;
        _prev += tracker.PreviousVoters.Contains(id) ? 1 : 0;
    }

    public bool Invoke() {
        var active = _cur >= tracker.CurrentVoters.Count / 2 + 1;

        if (tracker.PreviousVoters.Count > 0) {
            active &= _prev >= tracker.PreviousVoters.Count / 2 + 1;
        }

        return active;
    }
}
