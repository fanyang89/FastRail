using System.Buffers.Binary;
using System.Diagnostics;
using System.Text;
using FastRail.Exceptions;
using FastRail.Protos;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RaftNET;
using RocksDbSharp;
using SystemException = FastRail.Exceptions.SystemException;

namespace FastRail.Server;

public class DataStore : IDisposable {
    private static readonly byte[] KeyStatPrefix = "/stat"u8.ToArray();
    private static readonly byte[] KeyDataPrefix = "/data"u8.ToArray();
    private static readonly byte[] KeySessionPrefix = "/sess/"u8.ToArray();
    private static readonly byte[] KeyZxid = "/zxid"u8.ToArray();
    private readonly RocksDb _db;
    private readonly ILogger<DataStore> _logger;
    private readonly Lock _sessionLock = new();
    private readonly Dictionary<long, InMemorySession> _sessions = new();
    private readonly WatcherManager _watcherManager = new();
    private long _nextSessionId;

    public DataStore(string dataDir, ILogger<DataStore>? logger = null) {
        _logger = logger ?? new NullLogger<DataStore>();
        var options = new DbOptions().SetCreateIfMissing();
        _db = RocksDb.Open(options, dataDir);
    }

    public long NextZxid {
        get {
            lock (KeyZxid) {
                var zxid = LastZxid + 1;
                LastZxid = zxid;
                return zxid;
            }
        }
    }

    public long LastZxid {
        get {
            lock (KeyZxid) {
                var buffer = _db.Get(KeyZxid);
                if (buffer != null) {
                    return ReadLong(buffer);
                }
                _db.Put(KeyZxid, LongBytes(0));
                return 0;
            }
        }
        private set {
            lock (KeyZxid) {
                _db.Put(KeyZxid, LongBytes(value));
            }
        }
    }

    public void Dispose() {
        _db.Dispose();
        GC.SuppressFinalize(this);
    }

    public void Start() {
        CreateRootNode();

        // loading sessions
        using var it = _db.NewIterator();
        for (it.SeekToFirst(); it.Valid(); it.Next()) {
            if (it.Key().StartsWith(KeySessionPrefix)) {
                var session = SessionEntry.Parser.ParseFrom(it.Value());
                lock (_sessions) {
                    _sessions[session.Id] = new InMemorySession(session, []);
                }
            }
        }
    }

    public void Stop() {}

    public void CreateNode(long zxid, CreateNodeTransaction txn) {
        var statBuffer = _db.Get(MakeStatKey(txn.Path));
        if (statBuffer != null) {
            throw new NodeExistsException(txn.Path);
        }

        var batch = new WriteBatch();
        var stat = new StatEntry {
            Czxid = zxid,
            Mzxid = zxid,
            Ctime = txn.Ctime,
            Mtime = txn.Ctime,
            DataLength = txn.Data.Length,
            Ttl = txn.Ttl,
            IsContainer = txn.IsContainer,
            EphemeralOwner = txn.EphemeralOwner
        };

        // update parent properties
        if (txn.Path != "/") {
            var parentPath = GetParentPath(txn.Path);
            var parentStatBuffer = _db.Get(MakeStatKey(parentPath));
            if (parentStatBuffer == null) {
                throw new SystemException($"parent not found, path={txn.Path}");
            }
            var parentStat = StatEntry.Parser.ParseFrom(parentStatBuffer);
            parentStat.NumChildren += 1;
            parentStat.Cversion += 1;
            parentStat.Pzxid = zxid;
            batch.Put(MakeStatKey(parentPath), parentStat.ToByteArray());
        }
        batch.Put(KeyZxid, LongBytes(zxid));
        batch.Put(MakeStatKey(txn.Path), stat.ToByteArray());
        batch.Put(MakeDataKey(txn.Path), txn.Data.ToByteArray());

        // submit memory changes
        if (txn.EphemeralOwner != 0) {
            lock (_sessionLock) {
                if (!_sessions.TryGetValue(txn.EphemeralOwner, out var value)) {
                    throw new SystemException(
                        $"Create ephemeral for non-existing session, session_id={txn.EphemeralOwner} path={txn.Path}");
                }
                value.Ephemerals.Add(txn.Path);
            }
        }

        // submit write batch
        _db.Write(batch, new WriteOptions().SetSync(true));

        _watcherManager.Trigger(txn.Path, WatcherEventType.EventNodeCreated);
    }

    public static string GetParentPath(string childPath) {
        var p = childPath.LastIndexOf('/');
        if (p < 0 || childPath == "/") {
            throw new ArgumentException("invalid child path", nameof(childPath));
        }
        return p == 0 ? "/" : childPath[..p];
    }

    public void RemoveNode(long zxid, DeleteNodeTransaction txn) {
        if (txn.Path == "/") {
            throw new ArgumentException("Can't delete root node", nameof(txn));
        }
        if (string.IsNullOrWhiteSpace(txn.Path)) {
            throw new ArgumentException("Path is empty", nameof(txn));
        }

        // read parent stat
        var parentPath = GetParentPath(txn.Path);
        var parentStatBuffer = _db.Get(MakeStatKey(parentPath));
        if (parentStatBuffer == null) {
            throw new SystemException($"parent not found, path={txn.Path}");
        }
        var parentStat = StatEntry.Parser.ParseFrom(parentStatBuffer);
        parentStat.NumChildren -= 1;

        var batch = new WriteBatch();
        if (parentStat is { NumChildren: <= 0, IsContainer: true }) {
            // delete empty container node
            batch.Delete(MakeStatKey(parentPath));
            batch.Delete(MakeDataKey(parentPath));
        } else {
            // update parent stat
            batch.Put(MakeStatKey(parentPath), parentStat.ToByteArray());
        }
        batch.Delete(MakeDataKey(txn.Path));
        batch.Delete(MakeStatKey(txn.Path));
        _db.Write(batch, new WriteOptions().SetSync(true));

        _watcherManager.Trigger(txn.Path, WatcherEventType.EventNodeDeleted);
    }

    public byte[]? GetNodeData(string path) {
        var key = MakeDataKey(path);
        return _db.Get(key);
    }

    public StatEntry? GetNodeStat(string path) {
        var key = MakeStatKey(path);
        var value = _db.Get(key);
        return value == null ? null : StatEntry.Parser.ParseFrom(value);
    }

    public void CreateSession(long zxid, CreateSessionTransaction txn) {
        var sessionId = txn.Session.Id;
        lock (_sessions) {
            _sessions.Add(sessionId, new InMemorySession(txn.Session, []));
            _db.Put(MakeSessionKey(sessionId), txn.Session.ToByteArray(), writeOptions: new WriteOptions().SetSync(true));
        }
    }

    public long CreateSession(TimeSpan sessionTimeout) {
        var sessionId = _nextSessionId++;
        var key = MakeSessionKey(sessionId);
        var session = new SessionEntry {
            Id = sessionId, Timeout = sessionTimeout.Milliseconds, LastLive = Time.CurrentTimeMillis()
        };
        lock (_sessions) {
            _sessions.Add(sessionId, new InMemorySession(session, []));
            _db.Put(key, session.ToByteArray(), writeOptions: new WriteOptions().SetSync(true));
        }
        return sessionId;
    }

    public void RemoveSession(long sessionId) {
        var sessionKey = MakeSessionKey(sessionId);
        lock (_sessions) {
            var session = _sessions[sessionId];
            _sessions.Remove(sessionId);

            var batch = new WriteBatch();
            batch.Delete(sessionKey);
            foreach (var path in session.Ephemerals) {
                batch.Delete(MakeDataKey(path));
                batch.Delete(MakeStatKey(path));
            }
            _db.Write(batch, new WriteOptions().SetSync(true));
        }
    }

    public void TouchSession(long sessionId) {
        var key = MakeSessionKey(sessionId);
        lock (_sessions) {
            var session = _sessions[sessionId];
            session.Session.LastLive = Time.CurrentTimeMillis();
            _db.Put(key, session.Session.ToByteArray(), writeOptions: new WriteOptions().SetSync(true));
        }
    }

    public List<string> GetChildren(string path, out StatEntry stat) {
        var prefix = MakeStatKey(path);
        var buffer = _db.Get(prefix);
        if (buffer == null) {
            throw new RailException(ErrorCodes.NoNode);
        }
        stat = StatEntry.Parser.ParseFrom(buffer);

        var depth = PathDepth(path);
        var children = new List<string>();
        using var it = _db.NewIterator();
        for (it.Seek(prefix); it.Valid(); it.Next()) {
            if (!it.Key().StartsWith(prefix)) {
                break;
            }
            var nodeDepth = PathDepth(it.Key());
            if (nodeDepth == depth + 1) {
                children.Add(Encoding.UTF8.GetString(it.Value()));
            } else if (nodeDepth > depth + 1) {
                break;
            }
        }
        return children;
    }

    public List<string> ListEphemeral(string? pathPrefix) {
        var results = new List<string>();
        using var it = _db.NewIterator();
        for (it.Seek(KeyStatPrefix); it.Valid(); it.Next()) {
            if (!it.Key().StartsWith(KeyStatPrefix)) {
                break;
            }
            var path = new ArraySegment<byte>(it.Key(), KeyStatPrefix.Length,
                it.Key().Length - KeyStatPrefix.Length);
            if (pathPrefix != null && !path.StartsWith(pathPrefix)) {
                continue;
            }
            var node = StatEntry.Parser.ParseFrom(it.Value());
            if (node.EphemeralOwner != 0) {
                results.Add(Encoding.UTF8.GetString(it.Key()));
            }
        }
        return results;
    }

    public int CountAllChildren() {
        var acc = 0;
        using var it = _db.NewIterator();
        for (it.Seek(KeyDataPrefix); it.Valid(); it.Next()) {
            if (!it.Key().StartsWith(KeyDataPrefix)) {
                break;
            }
            acc++;
        }
        return acc;
    }

    public int CountAllChildren(string requestPath) {
        var acc = 0;
        var prefix = ByteArrayUtil.Concat(KeyDataPrefix, requestPath);
        using var it = _db.NewIterator();
        for (it.Seek(prefix); it.Valid(); it.Next()) {
            if (!it.Key().StartsWith(prefix)) {
                break;
            }
            var path = new ArraySegment<byte>(it.Key(), KeyDataPrefix.Length,
                it.Key().Length - KeyDataPrefix.Length);
            if (requestPath != null && path.StartsWith(requestPath) && path.Count > requestPath.Length) {
                acc++;
            }
        }
        return acc;
    }

    public string LastPathByPrefix(string requestPathPrefix) {
        var prefix = ByteArrayUtil.Concat(KeyDataPrefix, requestPathPrefix);
        using var it = _db.NewIterator();
        byte[]? lastKey = null;
        for (it.Seek(prefix); it.Valid(); it.Next()) {
            if (!prefix.StartsWith(prefix)) {
                break;
            }
            lastKey = it.Key();
        }
        return lastKey == null ? "" : Encoding.UTF8.GetString(lastKey);
    }

    public void UpdateNode(long zxid, UpdateNodeTransaction txn) {
        var statBuffer = _db.Get(MakeStatKey(txn.Path));
        if (statBuffer == null) {
            throw new RailException(ErrorCodes.NoNode);
        }

        var stat = StatEntry.Parser.ParseFrom(statBuffer);
        var statUpdated = txn.Mtime != 0 || txn.Version != 0 ||
                          txn.Cversion != 0 || txn.Aversion != 0 || txn.Pzxid != 0;
        if (txn.Mtime != 0) {
            stat.Mtime = txn.Mtime;
        }
        if (txn.Version != 0) {
            stat.Version = txn.Version;
        }
        if (txn.Cversion != 0) {
            stat.Cversion = txn.Cversion;
        }
        if (txn.Aversion != 0) {
            stat.Aversion = txn.Aversion;
        }
        if (txn.Pzxid != 0) {
            stat.Pzxid = txn.Pzxid;
        }

        var batch = new WriteBatch();
        if (statUpdated) {
            batch.Put(MakeStatKey(txn.Path), stat.ToByteArray());
        }
        if (txn.Data != null) {
            batch.Put(MakeDataKey(txn.Path), txn.Data.Span);
        }
        _db.Write(batch, new WriteOptions().SetSync(true));

        _watcherManager.Trigger(txn.Path, WatcherEventType.EventNodeDataChanged);
    }

    // create root node if not exists, zxid=0
    private void CreateRootNode() {
        var rootValue = _db.Get(MakeDataKey("/"));
        if (rootValue != null) {
            return;
        }
        CreateNode(0, new CreateNodeTransaction { Path = "/", Data = ByteString.Empty, Ctime = Time.CurrentTimeMillis() });
    }

    private static byte[] LongBytes(long value) {
        var buffer = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(buffer, value);
        return buffer;
    }

    private static long ReadLong(byte[] buffer) {
        Debug.Assert(buffer.Length == sizeof(long));
        return BinaryPrimitives.ReadInt64BigEndian(buffer);
    }

    private static byte[] MakeSessionKey(long sessionId) {
        return ByteArrayUtil.Concat(KeySessionPrefix, LongBytes(sessionId));
    }

    private static byte[] MakeDataKey(string path) {
        return ByteArrayUtil.Concat(KeyDataPrefix, path);
    }

    private static byte[] MakeStatKey(string path) {
        return ByteArrayUtil.Concat(KeyStatPrefix, path);
    }

    private static int PathDepth(string path) {
        // '/' for 0, '/path' for 1, '/path/123' for 2
        return path == "/" ? 0 : path.Count(x => x == '/');
    }

    private static int PathDepth(byte[] path) {
        if (path.Length == 1 && path[0] == '/') {
            return 0;
        }
        return path.Count(x => x == '/');
    }
}
