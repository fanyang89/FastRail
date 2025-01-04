using System.Buffers.Binary;
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
    private static readonly byte[] KeyDataNodePrefix = "/zdat"u8.ToArray();
    private static readonly byte[] KeySessionPrefix = "/sess/"u8.ToArray();
    private static readonly byte[] KeyZxid = "/zxid"u8.ToArray();
    private readonly CancellationTokenSource _cts = new();
    private readonly RocksDb _db;

    private readonly ILogger<DataStore> _logger;

    // track TTL/container nodes, -1 for container node, otherwise is TTL
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
        _backgroundScanTask?.Wait();
    }

    public void Start() {
        // read zxid buffer or initialize
        var zxidBuffer = _db.Get(KeyZxid);
        if (zxidBuffer == null) {
            zxidBuffer = new byte[sizeof(long)];
            BinaryPrimitives.WriteInt64BigEndian(zxidBuffer, 0);
            _db.Put(KeyZxid, zxidBuffer);
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
                var node = DataNodeMetadata.Parser.ParseFrom(it.Value());
                var path = Encoding.UTF8.GetString(it.Key().TrimPrefix(KeyDataNodeMetaPrefix));
                if (node.IsContainer) {
                    _nodeTracks.Add(path, -1);
                } else if (node.Ttl > 0) {
                    _nodeTracks.Add(path, node.Ttl);
                }
            }

        // start background scan task
        _backgroundScanTask = Task.Run(BackgroundScan(_cts.Token));
    }

    public void CreateNode(string path, byte[] data, long zxid, StatEntry stat, long? ttl = null, bool isContainer = false) {
        var keyBytes = ByteArrayUtil.Concat(KeyDataNodePrefix, path);
        var buffer = _db.Get(keyBytes);
        if (buffer != null) {
            throw new NodeExistsException(path);
        }

        var node = new DataNodeEntry { Data = ByteString.CopyFrom(data), Stat = stat };
        var nodeMeta = new DataNodeMetadata { IsContainer = isContainer };
        if (ttl != null) {
            nodeMeta.Ttl = ttl.Value;
        }

        var batch = new WriteBatch();
        batch.Put(keyBytes, node.ToByteArray());
        batch.Put(KeyZxid, CreateBufferForLong(zxid));
        if (ttl != null || isContainer) {
            var metadataKey = ByteArrayUtil.Concat(KeyDataNodeMetaPrefix, path);
            batch.Put(metadataKey, nodeMeta.ToByteArray());
        }
        _db.Write(batch, new WriteOptions().SetSync(true));

        if (ttl != null) {
            _nodeTracks.Add(path, ttl.Value);
        } else if (isContainer) {
            _nodeTracks.Add(path, -1);
        }
    }

    public void RemoveNode(string path) {
        var key = ByteArrayUtil.Concat(KeyDataNodePrefix, path);
        _db.Remove(key);
    }

    public int CountNodeChildren(string key) {
        var acc = 0;
        var depth = key.Count(x => x == '/');
        var keyBytes = key.ToBytes();
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

    public void UpdateNode(string key, byte[]? data, StatEntry? stat) {
        var keyBytes = ByteArrayUtil.Concat(KeyDataNodePrefix, key);
        var buffer = _db.Get(keyBytes);

        if (buffer == null) {
            throw new RailException(ErrorCodes.NoNode);
        }

        var node = DataNodeEntry.Parser.ParseFrom(buffer);

        if (data != null) {
            node.Data = ByteString.CopyFrom(data);
        }

        if (stat != null) {
            node.Stat = stat;
        }

        _db.Put(keyBytes, node.ToByteArray());
    }

    public DataNodeEntry? GetNode(string key) {
        var keyBytes = ByteArrayUtil.Concat(KeyDataNodePrefix, key);
        var value = _db.Get(keyBytes);
        if (value == null) {
            return null;
        }
        var node = DataNodeEntry.Parser.ParseFrom(value);
        return node;
    }

    public List<SessionEntry> GetSessions() {
        lock (_sessions) {
            return _sessions.Values.ToList();
        }
    }

    public int GetEphemeralCount() {
        using var it = _db.NewIterator();

        var acc = 0;

        for (it.Seek(KeyDataNodePrefix); it.Valid(); it.Next()) {
            if (!it.Key().StartsWith(KeyDataNodePrefix)) {
                break;
            }

            var node = DataNodeEntry.Parser.ParseFrom(it.Value());

            if (node.Stat.EphemeralOwner != 0) {
                acc++;
            }
        }

        return acc;
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
        var idBuffer = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(idBuffer, sessionId);
        var key = ByteArrayUtil.Concat(KeySessionPrefix, idBuffer);
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
        var idBuffer = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(idBuffer, sessionId);
        var key = ByteArrayUtil.Concat(KeySessionPrefix, idBuffer);

        lock (_sessions) {
            _db.Remove(key, writeOptions: new WriteOptions().SetSync(true));
            _sessions.Remove(sessionId);
        }
    }

    public void TouchSession(long sessionId) {
        var idBuffer = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(idBuffer, sessionId);
        var key = ByteArrayUtil.Concat(KeySessionPrefix, idBuffer);

        lock (_sessions) {
            var session = _sessions[sessionId];
            session.LastLive = Time.CurrentTimeMillis();
            _db.Put(key, session.ToByteArray(), writeOptions: new WriteOptions().SetSync(true));
            _sessions[sessionId] = session;
        }
    }

    public List<string> GetChildren(string path, out StatEntry stat) {
        var children = new List<string>();
        var prefix = ByteArrayUtil.Concat(KeyDataNodePrefix, path);
        var buffer = _db.Get(prefix);

        if (buffer == null) {
            throw new RailException(ErrorCodes.NoNode);
        }

        var node = DataNodeEntry.Parser.ParseFrom(buffer);
        stat = node.Stat;

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

    public List<string> ListEphemeral(string? prefix) {
        var results = new List<string>();
        using var it = _db.NewIterator();
        for (it.Seek(KeyDataNodePrefix); it.Valid(); it.Next()) {
            if (!it.Key().StartsWith(KeyDataNodePrefix)) {
                break;
            }
            var path = new ArraySegment<byte>(it.Key(), KeyDataNodePrefix.Length,
                it.Key().Length - KeyDataNodePrefix.Length);
            if (prefix != null && !path.StartsWith(prefix)) {
                continue;
            }
            var node = DataNodeEntry.Parser.ParseFrom(it.Value());
            if (node.Stat.EphemeralOwner != 0) {
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

    public string LastPrefixPath(string requestPathPrefix) {
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
            while (!token.IsCancellationRequested) await Task.Delay(_scanTickInterval, token);
            _logger.LogInformation("Data store background task exited");
        };
    }

    private static byte[] CreateBufferForLong(long value) {
        var buffer = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(buffer, value);
        return buffer;
    }

    private byte[] CreateSessionKey(long sessionId) {
        var buffer = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(buffer, sessionId);
        return ByteArrayUtil.Concat(KeySessionPrefix, buffer);
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