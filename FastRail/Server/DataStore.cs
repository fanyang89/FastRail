using System.Text;
using FastRail.Exceptions;
using FastRail.Jutes;
using FastRail.Jutes.Data;
using RocksDbSharp;

namespace FastRail.Server;

public class DataStore : IDisposable {
    private readonly RocksDb _db;
    private readonly Dictionary<long, ISet<string>> _ephemeralDict = new();

    private static byte[] _keyDataNodePrefix = "/data/"u8.ToArray();
    private static byte[] _keySessionPrefix = "/sess/"u8.ToArray();
    private static byte[] _keyZXid = "/zxid"u8.ToArray();

    public DataStore(string dataDir) {
        var options = new DbOptions().SetCreateIfMissing();
        _db = RocksDb.Open(options, dataDir);
    }

    public long LastZxid { get; set; }

    public void Dispose() {
        _db.Dispose();
    }

    public void CreateNode(string key, byte[] data, long acl) {
        var keyBytes = key.ToBytes();
        var buffer = _db.Get(keyBytes);

        if (buffer != null) {
            throw new NodeExistsException(key);
        }
        var node = new DataNode(data, acl, new StatPersisted {
            CZxid = 0,
            MZxid = 0,
            CTime = 0,
            MTime = 0,
            Version = 0,
            CVersion = 0,
            AVersion = 0,
            EphemeralOwner = 0,
            PZxid = 0
        });
        _db.Put(keyBytes, JuteSerializer.Serialize(node));
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

    public void UpdateNode(string key, byte[]? data, long? acl) {
        DataNode node;
        var keyBytes = Encoding.UTF8.GetBytes(key);
        var buffer = _db.Get(keyBytes);

        if (buffer != null) {
            node = JuteDeserializer.Deserialize<DataNode>(buffer);

            if (data != null) {
                node.Data = data;
            }

            if (acl != null) {
                node.ACL = acl.Value;
            }
        } else {
            node = new DataNode();
        }
        _db.Put(keyBytes, JuteSerializer.Serialize(node));
    }

    public DataNode? GetNode(string key) {
        var value = _db.Get(Encoding.UTF8.GetBytes(key));
        return value == null ? null : JuteDeserializer.Deserialize<DataNode>(value);
    }

    public ISet<long> GetSessions() {
        lock (_ephemeralDict) {
            return new SortedSet<long>(_ephemeralDict.Keys);
        }
    }

    public int GetEphemeralCount() {
        lock (_ephemeralDict) {
            return _ephemeralDict.Values.Sum(ephemeralSet => ephemeralSet.Count);
        }
    }

    public void RemoveEphemeral(long sessionId) {
        lock (_ephemeralDict) {
            _ephemeralDict.Remove(sessionId);
        }
    }
}