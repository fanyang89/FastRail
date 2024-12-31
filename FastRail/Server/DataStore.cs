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
    private readonly static byte[] KeyACLPrefix = "/acls"u8.ToArray();
    private readonly static byte[] KeySessionPrefix = "/sess/"u8.ToArray();
    private readonly static byte[] KeyDataNodePrefix = "/zdat"u8.ToArray();
    private readonly static byte[] KeyZxid = "/zxid"u8.ToArray();

    public DataStore(string dataDir) {
        var options = new DbOptions().SetCreateIfMissing();
        _db = RocksDb.Open(options, dataDir);
    }

    public void Dispose() {
        _db.Dispose();
    }

    public void Load() {
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
        var keyBytes = Encoding.UTF8.GetBytes(key);
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

    public DataNode? GetNode(string key) {
        var value = _db.Get(Encoding.UTF8.GetBytes(key));
        return value == null ? null : JuteDeserializer.Deserialize<DataNode>(value);
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
}