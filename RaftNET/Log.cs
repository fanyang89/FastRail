﻿using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace RaftNET;

public class Log {
    private ulong _firstIdx;
    private ulong _lastConfIdx;
    private readonly List<LogEntry> _log;
    private readonly ILogger<Log> _logger;
    private int _memoryUsage;
    private ulong _prevConfIdx;
    private SnapshotDescriptor _snapshot;
    private ulong _stableIdx;

    public Log(SnapshotDescriptor snapshot, List<LogEntry> logEntries, ILogger<Log> logger) {
        _logger = logger;
        _snapshot = snapshot;
        _log = logEntries;

        if (_log.Count == 0) {
            _firstIdx = snapshot.Idx + 1;
        } else {
            _firstIdx = _log.First().Idx;
            Debug.Assert(_firstIdx <= snapshot.Idx + 1);
        }

        _memoryUsage = RangeMemoryUsage(0, _log.Count);
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

    private LogEntry GetEntry(ulong idx) {
        return _log[(int)(idx - _firstIdx)];
    }

    public bool Empty() {
        return _log.Count == 0;
    }

    public Tuple<int, ulong> ApplySnapshot(SnapshotDescriptor snp, ulong maxTrailingEntries, int maxTrailingBytes) {
        Debug.Assert(snp.Idx > _snapshot.Idx);

        int releasedMemory;
        var idx = snp.Idx;

        if (idx > LastIdx()) {
            releasedMemory = _memoryUsage;
            _memoryUsage = 0;
            _log.Clear();
            _firstIdx = idx + 1;
        } else {
            var entriesToRemove = (ulong)_log.Count - (LastIdx() - idx);
            var trailingBytes = 0;

            for (var i = 0; i < maxTrailingBytes && entriesToRemove > 0; i++) {
                trailingBytes += MemoryUsageOf(_log[(int)(entriesToRemove - 1)]);

                if (trailingBytes > maxTrailingBytes) {
                    break;
                }

                --entriesToRemove;
            }

            releasedMemory = RangeMemoryUsage(0, (int)entriesToRemove);
            _log.RemoveRange(0, (int)entriesToRemove);
            _memoryUsage -= releasedMemory;
            _firstIdx += entriesToRemove;
        }

        _stableIdx = ulong.Max(idx, _stableIdx);

        if (idx >= _prevConfIdx) {
            _prevConfIdx = 0;

            if (idx >= _lastConfIdx) {
                _lastConfIdx = 0;
            }
        }

        _snapshot = snp;
        return new Tuple<int, ulong>(releasedMemory, _firstIdx);
    }

    public ulong MaybeAppend(IList<LogEntry> entries) {
        Debug.Assert(entries.Count > 0);

        var lastNewIdx = entries.Last().Idx;

        foreach (var entry in entries) {
            if (entry.Idx <= LastIdx()) {
                if (entry.Idx < _firstIdx) {
                    _logger.LogTrace(
                        "append_entries: skipping entry with idx {idx} less than log start {firstIdx}", entry.Idx,
                        _firstIdx);
                    continue;
                }

                if (entry.Term == GetEntry(entry.Idx).Term) {
                    _logger.LogTrace("append_entries: entries with index {idx} has matching terms {term}", entry.Idx,
                        entry.Term);
                    continue;
                }

                _logger.LogTrace(
                    "append_entries: entries with index {idx} has non matching terms e.term={term}, _log[i].term = {entryTerm}",
                    entry.Idx, entry.Term, GetEntry(entry.Idx).Term);
                Debug.Assert(entry.Idx > _snapshot.Idx);
                TruncateUncommitted(entry.Idx);
            }

            Debug.Assert(entry.Idx == NextIdx());
            Add(entry);
        }

        return lastNewIdx;
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

    public void Add(LogEntry entry) {
        _log.Add(entry);

        if (_log.Last().Configuration == null) {
            return;
        }

        _prevConfIdx = _lastConfIdx;
        _lastConfIdx = LastIdx();
    }

    public ulong LastIdx() {
        return (ulong)_log.Count + _firstIdx - 1;
    }

    public ulong NextIdx() {
        return LastIdx() + 1;
    }

    private int MemoryUsageOf(LogEntry entry) {
        if (entry.Command == null) {
            return 0;
        }

        var bufferSize = 0;

        if (entry.Command.Buffer != null) {
            bufferSize = entry.Command.Buffer.Length;
        }

        return 2 * sizeof(ulong) + bufferSize;
    }

    private int RangeMemoryUsage(int first, int last) {
        var result = 0;

        for (var i = first; i < last; i++) result += MemoryUsageOf(_log[i]);

        return result;
    }

    private void TruncateUncommitted(ulong idx) {
        Debug.Assert(idx >= _firstIdx);
        var it = (int)(idx - _firstIdx);
        var releasedMemory = RangeMemoryUsage(it, _log.Count);
        _log.RemoveRange(it, _log.Count - it + 1);
        _memoryUsage -= releasedMemory;
        StableTo(ulong.Min(_stableIdx, LastIdx()));

        if (_lastConfIdx > LastIdx()) {
            Debug.Assert(_prevConfIdx > _lastConfIdx);
            _lastConfIdx = _prevConfIdx;
            _prevConfIdx = 0;
        }
    }

    public void StableTo(ulong idx) {
        Debug.Assert(idx <= LastIdx());
        _stableIdx = idx;
    }

    public bool IsUpToUpdate(ulong idx, ulong term) {
        return term > LastTerm() || (term == LastTerm() && idx >= LastIdx());
    }

    public ulong LastTerm() {
        return _log.Count == 0 ? _snapshot.Term : _log.Last().Term;
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

    public ulong LastConfIdx() {
        return _lastConfIdx > 0 ? _lastConfIdx : _snapshot.Idx;
    }

    public Configuration GetConfiguration() {
        return _lastConfIdx > 0 ? _log[(int)(_lastConfIdx - _firstIdx)].Configuration : _snapshot.Config;
    }

    public Configuration LastConfFor(ulong idx) {
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

    public Configuration? GetPreviousConfiguration() {
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