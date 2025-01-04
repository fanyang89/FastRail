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
    private readonly RocksDb _db;
    private readonly ILogger<DataStore> _logger;
    private readonly TimeSpan _scanTickInterval;
    private Task? _backgroundScanTask;
    private readonly CancellationTokenSource _cts = new();
    private readonly Dictionary<long, SessionEntry> _sessions = new();
    private static readonly byte[] KeyACLPrefix = "/acls"u8.ToArray();
    private static readonly byte[] KeySessionPrefix = "/sess/"u8.ToArray();
    private static readonly byte[] KeyDataNodePrefix = "/zdat"u8.ToArray();
    private static readonly byte[] KeyZxid = "/zxid"u8.ToArray();
    private long _nextSessionId;

    public DataStore(string dataDir, TimeSpan scanTickInterval, ILogger<DataStore> logger) {
        _logger = logger;
        var options = new DbOptions().SetCreateIfMissing();
        _db = RocksDb.Open(options, dataDir);
        _scanTickInterval = scanTickInterval;
    }

    public void Dispose() {
        _db.Dispose();
        GC.SuppressFinalize(this);
    }


    public void Stop() {
        _cts.Cancel();
        _backgroundScanTask?.Wait();
    }

    private Func<Task> BackgroundScan(CancellationToken token) {
        return async () => {
            _logger.LogInformation("Data store background task started");
            while (!token.IsCancellationRequested) {
                await Task.Delay(_scanTickInterval, token);
            }
            _logger.LogInformation("Data store background task exited");
        };
    }

    public void Start() {
        // read zxid buffer or initialize
        var zxidBuffer = _db.Get(KeyZxid);
        if (zxidBuffer == null) {
            zxidBuffer = new byte[sizeof(long)];
            BinaryPrimitives.WriteInt64BigEndian(zxidBuffer, 0);
            _db.Put(KeyZxid, zxidBuffer);
        }

        // load sessions
        using var it = _db.NewIterator();
        for (it.SeekToFirst(); it.Valid(); it.Next()) {
            var buffer = it.Value();
            if (!it.Key().StartsWith(KeySessionPrefix)) {
                continue;
            }
            var session = SessionEntry.Parser.ParseFrom(buffer);
            lock (_sessions) {
                _sessions[session.Id] = session;
            }
        }

        // start background scan task
        _backgroundScanTask = Task.Run(BackgroundScan(_cts.Token));
    }

    public long NextZxid => LastZxid + 1;

    public long LastZxid {
        get {
            var buffer = _db.Get(KeyZxid);
            return BinaryPrimitives.ReadInt64BigEndian(buffer);
        }
    }

    private static byte[] CreateBufferForLong(long value) {
        var buffer = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(buffer, value);
        return buffer;
    }

    public void CreateNode(string key, byte[] data, StatEntry stat, long zxid) {
        var keyBytes = ByteArrayUtil.Concat(KeyDataNodePrefix, key);
        var buffer = _db.Get(keyBytes);

        if (buffer != null) {
            throw new NodeExistsException(key);
        }

        var node = new DataNodeEntry {
            Data = ByteString.CopyFrom(data),
            Stat = stat,
            Mode = CreateMode.Persistent, // TODO
            TTL = 0
        };

        var batch = new WriteBatch();
        batch.Put(keyBytes, node.ToByteArray());
        batch.Put(KeyZxid, CreateBufferForLong(zxid));
        _db.Write(batch, writeOptions: new WriteOptions().SetSync(true));
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

    private byte[] CreateSessionKey(long sessionId) {
        var buffer = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(buffer, sessionId);
        return ByteArrayUtil.Concat(KeySessionPrefix, buffer);
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
            Id = sessionId,
            Timeout = sessionTimeout.Milliseconds,
            LastLive = Time.CurrentTimeMillis()
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
}