using System.Diagnostics;

namespace RaftNET;

public class Log {
    private SnapshotDescriptor snapshot_;
    private IList<LogEntry> log_;
    private ulong firstIdx_;
    private ulong stableIdx_;
    private ulong lastConfIdx_;
    private ulong prevConfIdx_;

    public Log(SnapshotDescriptor snapshot, IList<LogEntry> logEntries) {
        snapshot_ = snapshot;
        log_ = logEntries;

        if (log_.Count == 0) {
            firstIdx_ = snapshot.Idx + 1;
        } else {
            firstIdx_ = log_.First().Idx;
        }
    }

    public LogEntry this[ulong index] {
        get {
            Debug.Assert(log_.Count > 0 && index >= firstIdx_);
            return log_[(int)(index - firstIdx_)];
        }
    }

    public void Add(LogEntry entry) {
        log_.Add(entry);

        if (log_.Last().Configuration == null) {
            return;
        }

        prevConfIdx_ = lastConfIdx_;
        lastConfIdx_ = LastIdx();
    }

    public ulong LastIdx() {
        return (ulong)log_.Count + firstIdx_ - 1;
    }

    public ulong NextIdx() {
        return LastIdx() + 1;
    }

    public void StableTo(ulong idx) {
        Debug.Assert(idx <= LastIdx());
        stableIdx_ = idx;
    }

    public bool IsUpToUpdate(ulong idx, ulong term) {
        return term > LastTerm() || (term == LastTerm() && idx >= LastIdx());
    }

    public ulong LastTerm() {
        return log_.Count == 0 ? snapshot_.Term : log_.Last().Term;
    }

    public ulong? TermFor(ulong idx) {
        if (log_.Count > 0 && idx >= firstIdx_) {
            return log_[(int)(idx - firstIdx_)].Term;
        }

        if (idx == snapshot_.Idx) {
            return snapshot_.Term;
        }

        return null;
    }

    public ulong LastConfIdx() {
        return lastConfIdx_ == 0 ? lastConfIdx_ : snapshot_.Idx;
    }

    public Configuration GetConfiguration() {
        return lastConfIdx_ > 0 ? log_[(int)(lastConfIdx_ - firstIdx_)].Configuration : snapshot_.Config;
    }

    public Configuration LastConfFor(ulong idx) {
        Debug.Assert(LastIdx() >= idx);
        Debug.Assert(idx >= snapshot_.Idx);

        if (lastConfIdx_ == 0) {
            Debug.Assert(prevConfIdx_ > 0);
            return snapshot_.Config;
        }

        if (idx >= lastConfIdx_) {
            return this[lastConfIdx_].Configuration;
        }

        if (prevConfIdx_ == 0) {
            return snapshot_.Config;
        }

        if (idx >= prevConfIdx_) {
            return this[prevConfIdx_].Configuration;
        }

        for (; idx > snapshot_.Idx; --idx) {
            if (this[idx].Configuration != null) {
                return this[idx].Configuration;
            }
        }

        return snapshot_.Config;
    }

    public Configuration? GetPreviousConfiguration() {
        if (prevConfIdx_ > 0) {
            return this[prevConfIdx_].Configuration;
        }

        return lastConfIdx_ > snapshot_.Idx ? snapshot_.Config : null;
    }

    private void InitLastConfigurationIdx() {
        for (var i = log_.Count - 1; i >= 0 && log_[i].Idx != snapshot_.Idx; --i) {
            if (log_[i].Configuration == null) {
                continue;
            }

            if (lastConfIdx_ == 0) {
                lastConfIdx_ = log_[i].Idx;
            } else {
                prevConfIdx_ = log_[i].Idx;
                break;
            }
        }
    }
}