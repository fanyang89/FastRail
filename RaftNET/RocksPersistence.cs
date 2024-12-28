using Google.Protobuf;
using RocksDbSharp;

namespace RaftNET;

public class RocksPersistence : IPersistence, IDisposable {
    private readonly RocksDb _db;
    private readonly byte[] _keyCommitIdx = "/a/commit-idx"u8.ToArray();
    private readonly byte[] _keySnapshot = "/a/snapshot"u8.ToArray();
    private readonly byte[] _keyTermVote = "/a/term-vote"u8.ToArray();
    private readonly byte[] _keyLogEntryPrefix = "/logs/"u8.ToArray();
    private readonly WriteOptions _syncWriteOption = new WriteOptions().SetSync(true);

    public RocksPersistence(string path) {
        var options = new DbOptions().SetCreateIfMissing();
        _db = RocksDb.Open(options, path);
    }

    public void Dispose() {
        lock (_keyCommitIdx)
        lock (_keySnapshot)
        lock (_keyTermVote)
        lock (_keyLogEntryPrefix) {
            _db.Dispose();
        }
    }

    public void StoreTermVote(ulong term, ulong vote) {
        lock (_keyTermVote) {
            var tv = new TermVote { Term = term, VotedFor = vote };
            var buf = tv.ToByteArray();
            var options = new WriteOptions();
            options.SetSync(true);
            _db.Put(_keyTermVote, buf, writeOptions: _syncWriteOption);
        }
    }

    public TermVote? LoadTermVote() {
        lock (_keyTermVote) {
            var buf = _db.Get(_keyTermVote);
            if (buf == null) {
                return null;
            }
            return TermVote.Parser.ParseFrom(buf);
        }
    }

    public void StoreCommitIdx(ulong idx) {
        lock (_keyCommitIdx) {
            var buf = BitConverter.GetBytes(idx);
            _db.Put(_keyCommitIdx, buf, writeOptions: _syncWriteOption);
        }
    }

    public ulong LoadCommitIdx() {
        lock (_keyCommitIdx) {
            var buf = _db.Get(_keyCommitIdx);
            return BitConverter.ToUInt64(buf);
        }
    }

    private int CountLogsUnlocked() {
        var count = 0;
        using var iter = _db.NewIterator();
        for (iter.Seek(_keyLogEntryPrefix); iter.Valid(); iter.Next()) {
            if (!iter.Key().StartsWith(_keyLogEntryPrefix)) {
                break;
            }
            ++count;
        }
        return count;
    }

    public void StoreSnapshotDescriptor(SnapshotDescriptor snapshot, ulong preserveLogEntries) {
        lock (_keySnapshot) {
            var buf = snapshot.ToByteArray();
            _db.Put(_keySnapshot, buf, writeOptions: _syncWriteOption);
            // preserve log entries
            var total = CountLogsUnlocked();
            if (preserveLogEntries >= (ulong)total) {
                return;
            }
            var batch = new WriteBatch();
            var rest = preserveLogEntries > 0 ? preserveLogEntries : ulong.MaxValue;
            {
                using var iter = _db.NewIterator();
                for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
                    if (!iter.Key().StartsWith(_keyLogEntryPrefix)) {
                        continue;
                    }
                    var logIdx = ByteArrayUtil.GetIdx(iter.Key(), _keyLogEntryPrefix);
                    if (logIdx > snapshot.Idx) {
                        break;
                    }
                    if (rest > 0) {
                        batch.Delete(iter.Key());
                        --rest;
                    }
                }
            }
            _db.Write(batch, _syncWriteOption);
        }
    }

    public SnapshotDescriptor LoadSnapshotDescriptor() {
        lock (_keySnapshot) {
            var buf = _db.Get(_keySnapshot);
            var snapshot = SnapshotDescriptor.Parser.ParseFrom(buf);
            return snapshot;
        }
    }

    public void StoreLogEntries(IEnumerable<LogEntry> entries) {
        lock (_keyLogEntryPrefix) {
            var batch = new WriteBatch();
            foreach (var entry in entries) {
                var key = ByteArrayUtil.Concat(_keyLogEntryPrefix, entry.Idx);
                batch.Put(key, entry.ToByteArray());
            }
            _db.Write(batch, _syncWriteOption);
        }
    }

    public IList<LogEntry> LoadLog() {
        var entries = new List<LogEntry>();
        lock (_keyLogEntryPrefix) {
            using var iter = _db.NewIterator();
            for (iter.Seek(_keyLogEntryPrefix); iter.Valid(); iter.Next()) {
                var entry = LogEntry.Parser.ParseFrom(iter.Value());
                entries.Add(entry);
            }
        }
        return entries;
    }

    public void TruncateLog(ulong idx) {
        lock (_keyLogEntryPrefix) {
            var batch = new WriteBatch();
            {
                using var iter = _db.NewIterator();
                var beginKey = ByteArrayUtil.Concat(_keyLogEntryPrefix, idx);
                for (iter.Seek(beginKey); iter.Valid(); iter.Next()) {
                    var key = iter.Key();
                    batch.Delete(iter.Key());
                }
            }
            _db.Write(batch, _syncWriteOption);
        }
    }
}