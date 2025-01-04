using System.Buffers.Binary;
using System.Text;
using FastRail.Exceptions;
using FastRail.Jutes;
using FastRail.Protos;
using Google.Protobuf;
using RaftNET;
using RocksDbSharp;

namespace FastRail.Server;

public class DataStore : IDisposable {
    private readonly RocksDb _db;
    private readonly Dictionary<long, SessionEntry> _sessions = new();
    private static readonly byte[] KeyACLPrefix = "/acls"u8.ToArray();
    private static readonly byte[] KeySessionPrefix = "/sess/"u8.ToArray();
    private static readonly byte[] KeyDataNodePrefix = "/zdat"u8.ToArray();
    private static readonly byte[] KeyZxid = "/zxid"u8.ToArray();

    public DataStore(string dataDir) {
        var options = new DbOptions().SetCreateIfMissing();
        _db = RocksDb.Open(options, dataDir);
    }

    public void Dispose() {
        _db.Dispose();
    }

    public void Load() {
        var zxidBuffer = _db.Get(KeyZxid);

        if (zxidBuffer == null) {
            zxidBuffer = new byte[sizeof(long)];
            BinaryPrimitives.WriteInt64BigEndian(zxidBuffer, 0);
            _db.Put(KeyZxid, zxidBuffer);
        }

        using var it = _db.NewIterator();

        for (it.SeekToFirst(); it.Valid(); it.Next()) {
            var buffer = it.Value();

            if (it.Key().StartsWith(KeySessionPrefix)) {
                var session = SessionEntry.Parser.ParseFrom(buffer);

                lock (_sessions) {
                    _sessions[session.Id] = session;
                }
            }
        }
    }

    public long NextZxid => LastZxid + 1;

    public long LastZxid {
        get {
            var buffer = _db.Get(KeyZxid);
            return BinaryPrimitives.ReadInt64BigEndian(buffer);
        }
        private set {
            var buffer = new byte[sizeof(long)];
            BinaryPrimitives.WriteInt64BigEndian(buffer, value);
            _db.Put(KeyZxid, buffer, writeOptions: new WriteOptions().SetSync(true));
        }
    }

    public void CreateNode(string key, byte[] data, StatEntry stat) {
        var keyBytes = ByteArrayUtil.Concat(KeyDataNodePrefix, key);
        var buffer = _db.Get(keyBytes);

        if (buffer != null) {
            throw new NodeExistsException(key);
        }

        var node = new DataNodeEntry {
            Data = ByteString.CopyFrom(data),
            Stat = stat
        };
        _db.Put(keyBytes, node.ToByteArray(), writeOptions: new WriteOptions().SetSync(true));
    }

    public void RemoveNode(string key) {
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

    public void CreateSession(SessionEntry session) {
        var idBuffer = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(idBuffer, session.Id);
        var key = ByteArrayUtil.Concat(KeySessionPrefix, idBuffer);

        lock (_sessions) {
            _db.Put(key, session.ToByteArray(), writeOptions: new WriteOptions().SetSync(true));
            _sessions.Add(session.Id, session);
        }
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
}