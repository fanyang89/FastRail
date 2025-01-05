using System.Buffers.Binary;
using System.Diagnostics;
using System.Text;
using FastRail.Exceptions;
using FastRail.Protos;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using RaftNET;
using RocksDbSharp;

namespace FastRail.Server;

public class DataStore : IDisposable {
    private static readonly byte[] KeyDataNodeMetaPrefix = "/zmta"u8.ToArray();
    private static readonly byte[] KeyDataNodeStatPrefix = "/stat"u8.ToArray();
    private static readonly byte[] KeyDataNodePrefix = "/zdat"u8.ToArray();
    private static readonly byte[] KeySessionPrefix = "/sess/"u8.ToArray();
    private static readonly byte[] KeyZxid = "/zxid"u8.ToArray();
    private readonly CancellationTokenSource _cts = new();
    private readonly RocksDb _db;
    private readonly ILogger<DataStore> _logger;
    private readonly Dictionary<string, long> _nodeTracks = new();
    private readonly TimeSpan _scanTickInterval;
    private readonly Dictionary<long, SessionEntry> _sessions = new();
    private Task? _backgroundScanTask;
    private long _nextSessionId;

    public DataStore(string dataDir, TimeSpan scanTickInterval, ILogger<DataStore> logger) {
        _logger = logger;
        var options = new DbOptions().SetCreateIfMissing();
        _db = RocksDb.Open(options, dataDir);
        _scanTickInterval = scanTickInterval;
    }

    public long NextZxid => LastZxid + 1;

    public long LastZxid {
        get {
            var buffer = _db.Get(KeyZxid);
            return BinaryPrimitives.ReadInt64BigEndian(buffer);
        }
    }

    public void Dispose() {
        _db.Dispose();
        GC.SuppressFinalize(this);
    }

    public void Stop() {
        _cts.Cancel();
        try {
            _backgroundScanTask?.Wait();
        }
        catch (AggregateException ex) {
            if (ex.InnerExceptions.Count != 1) {
                throw;
            }
            if (ex.InnerException is not TaskCanceledException) {
                throw;
            }
        }
    }

    public void Start() {
        // read zxid buffer or initialize
        var zxidBuffer = _db.Get(KeyZxid);
        if (zxidBuffer == null) {
            zxidBuffer = new byte[sizeof(long)];
            BinaryPrimitives.WriteInt64BigEndian(zxidBuffer, 0);
            _db.Put(KeyZxid, zxidBuffer);
        }

        // create root node if not exists
        var rootValue = _db.Get(CreateDataNodeKey("/"));
        if (rootValue == null) {
            var now = Time.CurrentTimeMillis();
            CreateNode("/", [], 0, new StatEntry {
                Czxid = 0,
                Mzxid = 0,
                Ctime = now,
                Mtime = now,
                Version = 0,
                Cversion = 0,
                Aversion = 0,
                EphemeralOwner = 0,
                Pzxid = 0,
                DataLength = 0,
                NumChildren = 0
            });
        }

        // loading data
        using var it = _db.NewIterator();
        for (it.SeekToFirst(); it.Valid(); it.Next())
            if (it.Key().StartsWith(KeySessionPrefix)) {
                var session = SessionEntry.Parser.ParseFrom(it.Value());
                lock (_sessions) {
                    _sessions[session.Id] = session;
                }
            } else if (it.Key().StartsWith(KeyDataNodeMetaPrefix)) {
                var node = DataNodeProperty.Parser.ParseFrom(it.Value());
                var path = Encoding.UTF8.GetString(it.Key().TrimPrefix(KeyDataNodeMetaPrefix));
                if (node.IsContainer) {
                    lock (_nodeTracks) {
                        _nodeTracks.Add(path, -1);
                    }
                } else if (node.Ttl > 0) {
                    lock (_nodeTracks) {
                        _nodeTracks.Add(path, node.Ttl);
                    }
                }
            }

        // start background scan task
        _backgroundScanTask = Task.Run(BackgroundScan(_cts.Token));
    }

    public void CreateNode(string path, byte[] data, long zxid, StatEntry stat, long? ttl = null, bool isContainer = false) {
        var key = CreateDataNodeKey(path);
        var buffer = _db.Get(key);
        if (buffer != null) {
            throw new NodeExistsException(path);
        }

        var nodeMeta = new DataNodeProperty { IsContainer = isContainer };
        if (ttl != null) {
            nodeMeta.Ttl = ttl.Value;
        }

        var batch = new WriteBatch();
        batch.Put(key, data);
        batch.Put(CreateStatKey(path), stat.ToByteArray());
        batch.Put(KeyZxid, CreateBufferForLong(zxid));
        if (ttl != null || isContainer) {
            var metadataKey = ByteArrayUtil.Concat(KeyDataNodeMetaPrefix, path);
            batch.Put(metadataKey, nodeMeta.ToByteArray());
        }
        _db.Write(batch, new WriteOptions().SetSync(true));

        if (ttl != null) {
            lock (_nodeTracks) {
                _nodeTracks.Add(path, ttl.Value);
            }
        } else if (isContainer) {
            lock (_nodeTracks) {
                _nodeTracks.Add(path, -1);
            }
        }

        UpdateParent(path);
    }

    public static string GetParentPath(string childPath) {
        var p = childPath.LastIndexOf('/');
        if (p < 0 || childPath == "/") {
            throw new ArgumentException("invalid child path", nameof(childPath));
        }
        return p == 0 ? "/" : childPath[..p];
    }

    private void UpdateParent(string childPath) {
        if (childPath == "/") {
            return;
        }
        var parentPath = GetParentPath(childPath);
        var parent = GetNodeStat(parentPath);
        if (parent == null) {
            throw new ApplicationException();
        }
        parent.NumChildren = CountNodeChildren(parentPath);
    }

    public void RemoveNode(string path) {
        _db.Remove(ByteArrayUtil.Concat(KeyDataNodePrefix, path));
        _db.Remove(ByteArrayUtil.Concat(KeyDataNodeMetaPrefix, path));
    }

    private int CountNodeChildren(string path) {
        var acc = 0;
        var depth = path.Count(x => x == '/');
        var keyBytes = path.ToBytes();
        using var it = _db.NewIterator();
        for (it.Seek(keyBytes); it.Valid(); it.Next()) {
            if (!it.Key().StartsWith(keyBytes)) {
                break;
            }
            var keyDepth = it.Key().Count(x => x == '/');
            if (keyDepth > depth + 1) {
                break;
            }
            if (keyDepth == depth + 1) {
                ++acc;
            }
        }
        return acc;
    }

    public byte[]? GetNodeData(string path) {
        var key = CreateDataNodeKey(path);
        return _db.Get(key);
    }

    public StatEntry? GetNodeStat(string path) {
        var key = CreateStatKey(path);
        var value = _db.Get(key);
        return value == null ? null : StatEntry.Parser.ParseFrom(value);
    }

    public void PutNodeStat(string path, StatEntry stat) {
        var key = CreateStatKey(path);
        _db.Put(key, stat.ToByteArray());
    }

    public void PutSession(SessionEntry session) {
        var key = CreateSessionKey(session.Id);
        lock (_sessions) {
            _db.Put(key, session.ToByteArray(), writeOptions: new WriteOptions().SetSync(true));
            _sessions.Add(session.Id, session);
        }
    }

    public long CreateSession(TimeSpan sessionTimeout) {
        var sessionId = _nextSessionId++;
        var key = CreateSessionKey(sessionId);
        var session = new SessionEntry {
            Id = sessionId, Timeout = sessionTimeout.Milliseconds, LastLive = Time.CurrentTimeMillis()
        };
        lock (_sessions) {
            _db.Put(key, session.ToByteArray(), writeOptions: new WriteOptions().SetSync(true));
            _sessions.Add(session.Id, session);
        }
        return sessionId;
    }

    public void RemoveSession(long sessionId) {
        var key = CreateSessionKey(sessionId);
        lock (_sessions) {
            _db.Remove(key, writeOptions: new WriteOptions().SetSync(true));
            _sessions.Remove(sessionId);
        }
    }

    public void TouchSession(long sessionId) {
        var key = CreateSessionKey(sessionId);
        lock (_sessions) {
            var session = _sessions[sessionId];
            session.LastLive = Time.CurrentTimeMillis();
            _db.Put(key, session.ToByteArray(), writeOptions: new WriteOptions().SetSync(true));
            _sessions[sessionId] = session;
        }
    }

    public List<string> GetChildren(string path, out StatEntry stat) {
        var children = new List<string>();
        var prefix = CreateStatKey(path);
        var buffer = _db.Get(prefix);

        if (buffer == null) {
            throw new RailException(ErrorCodes.NoNode);
        }

        stat = StatEntry.Parser.ParseFrom(buffer);

        using var it = _db.NewIterator();
        var depth = PathDepth(path);

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
        for (it.Seek(KeyDataNodeStatPrefix); it.Valid(); it.Next()) {
            if (!it.Key().StartsWith(KeyDataNodeStatPrefix)) {
                break;
            }
            var path = new ArraySegment<byte>(it.Key(), KeyDataNodeStatPrefix.Length,
                it.Key().Length - KeyDataNodeStatPrefix.Length);
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
        for (it.Seek(KeyDataNodePrefix); it.Valid(); it.Next()) {
            if (!it.Key().StartsWith(KeyDataNodePrefix)) {
                break;
            }
            acc++;
        }
        return acc;
    }

    public int CountAllChildren(string requestPath) {
        var acc = 0;
        var prefix = ByteArrayUtil.Concat(KeyDataNodePrefix, requestPath);
        using var it = _db.NewIterator();
        for (it.Seek(prefix); it.Valid(); it.Next()) {
            if (!it.Key().StartsWith(prefix)) {
                break;
            }
            var path = new ArraySegment<byte>(it.Key(), KeyDataNodePrefix.Length,
                it.Key().Length - KeyDataNodePrefix.Length);
            if (requestPath != null && path.StartsWith(requestPath) && path.Count > requestPath.Length) {
                acc++;
            }
        }
        return acc;
    }

    public string LastPathByPrefix(string requestPathPrefix) {
        var prefix = ByteArrayUtil.Concat(KeyDataNodePrefix, requestPathPrefix);
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

    private Func<Task> BackgroundScan(CancellationToken token) {
        return async () => {
            _logger.LogInformation("Data store background task started");
            while (!token.IsCancellationRequested) {
                await Task.Delay(_scanTickInterval, token);
                ScanNodes();
            }
            _logger.LogInformation("Data store background task exited");
        };
    }

    private void ScanNodes() {
        var expired = new List<string>();
        lock (_nodeTracks) {
            foreach (var (path, ttl) in _nodeTracks) {
                if (ttl == -1) {
                    // it's container
                    var children = CountNodeChildren(path);
                    if (children <= 0) {
                        expired.Add(path);
                    }
                    _logger.LogInformation("Removing container node, path={}", path);
                } else if (ttl > 0) {
                    if (ttl - _scanTickInterval.Milliseconds > 0) {
                        _nodeTracks[path] -= _scanTickInterval.Milliseconds;
                    } else {
                        _logger.LogInformation("Removing TTL node, path={}", path);
                        expired.Add(path);
                    }
                }
            }

            foreach (var path in expired) {
                _nodeTracks.Remove(path);
                RemoveNode(path);
            }
        }
    }

    private static byte[] CreateBufferForLong(long value) {
        var buffer = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(buffer, value);
        return buffer;
    }

    private static long ReadLong(byte[] buffer) {
        Debug.Assert(buffer.Length == sizeof(long));
        return BinaryPrimitives.ReadInt64BigEndian(buffer);
    }

    private static byte[] CreateSessionKey(long sessionId) {
        var buffer = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(buffer, sessionId);
        return ByteArrayUtil.Concat(KeySessionPrefix, buffer);
    }

    private static byte[] CreateDataNodeKey(string path) {
        return ByteArrayUtil.Concat(KeyDataNodePrefix, path);
    }

    private static byte[] CreateStatKey(string path) {
        return ByteArrayUtil.Concat(KeyDataNodeStatPrefix, path);
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