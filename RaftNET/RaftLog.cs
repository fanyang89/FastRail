﻿using System.Diagnostics;
using Google.Protobuf;
using Serilog;

namespace RaftNET;

public class RaftLog : IDeepCloneable<RaftLog> {
    private readonly List<LogEntry> _log;
    private ulong _firstIdx;
    private ulong _lastConfIdx;
    private int _memoryUsage;
    private ulong _prevConfIdx;
    private SnapshotDescriptor _snapshot;
    private ulong _stableIdx;

    public RaftLog(SnapshotDescriptor? snapshot, List<LogEntry>? logEntries = null) {
        _snapshot = snapshot ?? new SnapshotDescriptor();
        _log = logEntries ?? [];

        if (_log.Count == 0) {
            _firstIdx = _snapshot.Idx + 1;
        } else {
            _firstIdx = _log.First().Idx;
            Debug.Assert(_firstIdx <= _snapshot.Idx + 1);
        }

        Debug.Assert(_firstIdx > 0);
        StableTo(LastIdx());
        InitLastConfigurationIdx();
    }

    public LogEntry this[ulong index] {
        get {
            Debug.Assert(_log.Count > 0 && index >= _firstIdx);
            return GetEntry(index);
        }
    }

    public ulong LastConfIdx => _lastConfIdx > 0 ? _lastConfIdx : _snapshot.Idx;

    public RaftLog Clone() {
        var logEntries = _log.Select(entry => entry.Clone()).ToList();
        var log = new RaftLog(_snapshot.Clone(), logEntries);
        return log;
    }

    public void Add(LogEntry entry) {
        _log.Add(entry);
        _memoryUsage += MemoryUsageOf(entry);

        if (_log.Last().Configuration != null) {
            _prevConfIdx = _lastConfIdx;
            _lastConfIdx = LastIdx();
        }
    }

    public (int, ulong) ApplySnapshot(SnapshotDescriptor snp, int maxTrailingEntries, int maxTrailingBytes) {
        Debug.Assert(snp.Idx > _snapshot.Idx);

        var releasedMemory = 0;
        var idx = snp.Idx;

        if (idx > LastIdx()) {
            _memoryUsage = 0;
            _log.Clear();
            _firstIdx = idx + 1;
        } else {
            var entriesToRemove = _log.Count - (int)(LastIdx() - idx);
            var trailingBytes = 0;

            for (var i = 0; i < maxTrailingEntries && entriesToRemove > 0; ++i) {
                trailingBytes += MemoryUsageOf(_log[entriesToRemove - 1]);

                if (trailingBytes > maxTrailingBytes) {
                    break;
                }

                --entriesToRemove;
            }

            releasedMemory = MemoryUsageOf(0, entriesToRemove);
            _log.RemoveRange(0, entriesToRemove);
            _memoryUsage -= releasedMemory;
            _firstIdx += (ulong)entriesToRemove;
        }

        _stableIdx = ulong.Max(idx, _stableIdx);

        if (idx >= _prevConfIdx) {
            _prevConfIdx = 0;

            if (idx >= _lastConfIdx) {
                _lastConfIdx = 0;
            }
        }

        _snapshot = snp;
        return (releasedMemory, _firstIdx);
    }

    public bool Empty() {
        return _log.Count == 0;
    }

    public Configuration GetConfiguration() {
        if (_lastConfIdx > 0) {
            var cfg = _log[(int)(_lastConfIdx - _firstIdx)].Configuration;
            return new Configuration(cfg);
        }

        if (_snapshot.Config == null) {
            return new Configuration();
        }

        return new Configuration(_snapshot.Config);
    }

    public Configuration? GetPreviousConfiguration() {
        var cfg = DoGetPreviousConfiguration();

        if (cfg == null) {
            return cfg;
        }

        return new Configuration(cfg);
    }

    public SnapshotDescriptor GetSnapshot() {
        return _snapshot;
    }

    public int InMemorySize() {
        return _log.Count;
    }

    public bool IsUpToUpdate(ulong idx, ulong term) {
        return term > LastTerm() || term == LastTerm() && idx >= LastIdx();
    }

    public Configuration LastConfFor(ulong idx) {
        return new Configuration(DoLastConfFor(idx));
    }

    public ulong LastIdx() {
        return (ulong)_log.Count + _firstIdx - 1;
    }

    public ulong LastTerm() {
        if (_log.Count == 0) {
            return _snapshot.Term;
        }
        return _log.Last().Term;
    }

    public Tuple<bool, ulong> MatchTerm(ulong idx, ulong term) {
        if (idx == 0) {
            return new Tuple<bool, ulong>(true, 0);
        }

        if (idx < _snapshot.Idx) {
            return new Tuple<bool, ulong>(true, LastTerm());
        }

        ulong myTerm;

        if (idx == _snapshot.Idx) {
            myTerm = _snapshot.Term;
        } else {
            var i = idx - _firstIdx;

            if (i >= (ulong)_log.Count) {
                return new Tuple<bool, ulong>(false, 0);
            }

            myTerm = _log[(int)i].Term;
        }

        return myTerm == term ? new Tuple<bool, ulong>(true, 0) : new Tuple<bool, ulong>(false, myTerm);
    }

    public ulong MaybeAppend(IList<LogEntry> entries) {
        Debug.Assert(entries.Count > 0);

        var lastNewIdx = entries.Last().Idx;

        foreach (var e in entries) {
            if (e.Idx <= LastIdx()) {
                if (e.Idx < _firstIdx) {
                    Log.Debug(
                        "append_entries: skipping entry with idx {idx} less than log start {firstIdx}", e.Idx,
                        _firstIdx);
                    continue;
                }

                if (e.Term == GetEntry(e.Idx).Term) {
                    Log.Debug("append_entries: entries with index {idx} has matching terms {term}", e.Idx, e.Term);
                    continue;
                }

                Log.Debug(
                    "append_entries: entries with index {idx} has non matching terms e.term={term}, _log[i].term = {entryTerm}",
                    e.Idx, e.Term, GetEntry(e.Idx).Term);
                Debug.Assert(e.Idx > _snapshot.Idx);
                TruncateUncommitted(e.Idx);
            }

            Debug.Assert(e.Idx == NextIdx());
            Add(e);
        }

        return lastNewIdx;
    }

    public int MemoryUsage() {
        return _memoryUsage;
    }

    public ulong NextIdx() {
        return LastIdx() + 1;
    }

    public ulong StableIdx() {
        return _stableIdx;
    }

    public void StableTo(ulong idx) {
        Debug.Assert(idx <= LastIdx());
        _stableIdx = idx;
    }

    public ulong? TermFor(ulong idx) {
        if (_log.Count > 0 && idx >= _firstIdx) {
            return _log[(int)(idx - _firstIdx)].Term;
        }

        if (idx == _snapshot.Idx) {
            return _snapshot.Term;
        }

        return null;
    }

    private LogEntry GetEntry(ulong idx) {
        return _log[(int)(idx - _firstIdx)];
    }

    private int MemoryUsageOf(LogEntry entry) {
        switch (entry.DataCase) {
            case LogEntry.DataOneofCase.Command:
                return entry.Command.CalculateSize();
            case LogEntry.DataOneofCase.Configuration:
            case LogEntry.DataOneofCase.Fake:
            case LogEntry.DataOneofCase.None:
                break;
        }

        return 0;
    }

    private void TruncateUncommitted(ulong idx) {
        Debug.Assert(idx >= _firstIdx);
        var it = (int)(idx - _firstIdx);
        var last = _log.Count - it;
        var releasedMemory = MemoryUsageOf(it, _log.Count);
        _log.RemoveRange(it, last);
        _memoryUsage -= releasedMemory;
        StableTo(ulong.Min(_stableIdx, LastIdx()));

        if (_lastConfIdx > LastIdx()) {
            Debug.Assert(_prevConfIdx < _lastConfIdx);
            _lastConfIdx = _prevConfIdx;
            _prevConfIdx = 0;
        }
    }

    private int MemoryUsageOf(int first, int last) {
        var usage = 0;

        for (var i = first; i < last; i++) usage += MemoryUsageOf(_log[i]);

        return usage;
    }

    private Configuration DoLastConfFor(ulong idx) {
        Debug.Assert(LastIdx() >= idx);
        Debug.Assert(idx >= _snapshot.Idx);

        if (_lastConfIdx == 0) {
            Debug.Assert(_prevConfIdx > 0);
            return _snapshot.Config;
        }

        if (idx >= _lastConfIdx) {
            return GetEntry(_lastConfIdx).Configuration;
        }

        if (_prevConfIdx == 0) {
            return _snapshot.Config;
        }

        if (idx >= _prevConfIdx) {
            return GetEntry(_prevConfIdx).Configuration;
        }

        for (; idx > _snapshot.Idx; --idx)
            if (this[idx].Configuration != null) {
                return GetEntry(idx).Configuration;
            }

        return _snapshot.Config;
    }

    private Configuration? DoGetPreviousConfiguration() {
        if (_prevConfIdx > 0) {
            return this[_prevConfIdx].Configuration;
        }

        return _lastConfIdx > _snapshot.Idx ? _snapshot.Config : null;
    }

    private void InitLastConfigurationIdx() {
        for (var i = _log.Count - 1; i >= 0 && _log[i].Idx != _snapshot.Idx; --i) {
            if (_log[i].Configuration == null) {
                continue;
            }

            if (_lastConfIdx == 0) {
                _lastConfIdx = _log[i].Idx;
            } else {
                _prevConfIdx = _log[i].Idx;
                break;
            }
        }
    }
}
