namespace RaftNET;

public class ActivityTracker {
    private Tracker _tracker;
    private int _cur;
    private int _prev;

    public ActivityTracker(Tracker tracker) {
        _tracker = tracker;
    }

    public void Record(ulong id) {
        _cur += _tracker.CurrentVoters.Contains(id) ? 1 : 0;
        _prev += _tracker.PreviousVoters.Contains(id) ? 1 : 0;
    }

    public bool Record() {
        var active = _cur >= _tracker.CurrentVoters.Count / 2 + 1;

        if (_tracker.PreviousVoters.Count > 0) {
            active &= _prev >= _tracker.PreviousVoters.Count / 2 + 1;
        }

        return active;
    }
};