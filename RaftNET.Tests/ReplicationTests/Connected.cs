using Google.Protobuf;

namespace RaftNET.Tests.ReplicationTests;

public class Connected : IDeepCloneable<Connected> {
    public ISet<Connection> Disconnected = new HashSet<Connection>();
    public ulong N;

    public Connected(ulong n) {
        N = n;
    }

    public Connected Clone() {
        return new Connected(N) {
            Disconnected = new HashSet<Connection>(Disconnected),
        };
    }

    public void Connect(ulong id) {
        Disconnected = Disconnected.Where(x => x.to != id && x.from != id).ToHashSet();
    }

    public void ConnectAll() {
        Disconnected.Clear();
    }

    public void Cut(ulong id1, ulong id2) {
        Disconnected.Add(new Connection(id1, id2));
        Disconnected.Add(new Connection(id2, id1));
    }

    public void Disconnect(ulong id, ulong? except = null) {
        for (ulong other = 0; other < N; ++other) {
            if (id != other && !(except != null && other == except.Value)) {
                Cut(id, other);
            }
        }
    }

    public bool IsConnected(ulong id1, ulong id2) {
        return !Disconnected.Contains(new Connection(id1, id2)) &&
               !Disconnected.Contains(new Connection(id2, id1));
    }
}
