using System.Text;
using FastRail.Jutes.Data;
using RocksDbSharp;

namespace FastRail.Server;

public class DataStore : IDisposable {
    private readonly RocksDb _db;

    public DataStore(string dataDir) {
        var options = new DbOptions().SetCreateIfMissing();
        _db = RocksDb.Open(options, dataDir);
    }

    public void Dispose() {
        _db.Dispose();
    }

    public byte[] Get(byte[] key) {
        return _db.Get(key);
    }

    public void Create(string key) {
        if (_db.Get(key) == null) {
            _db.Put(Encoding.UTF8.GetBytes(key), Array.Empty<byte>());
        }
    }

    public void Delete(string key) {
        _db.Remove(Encoding.UTF8.GetBytes(key));
    }

    public Stat? Exists(string key) {
        return null;
    }
}