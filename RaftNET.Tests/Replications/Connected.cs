using Google.Protobuf;

namespace RaftNET.Tests.Replications;

public class Connected : IDeepCloneable<Connected> {
    public ISet<Connection> Disconnected = new HashSet<Connection>();
    public int N;

    public Connected(int n) {
        N = n;
    }

    public void Cut(int id1, int id2) {
        Disconnected.Add(new Connection(id1, id2));
        Disconnected.Add(new Connection(id2, id1));
    }

    public void Disconnect(int id, int? except = null) {
        for (var other = 0; other < N; ++other) {
            if (id != other && !(except != null && other == except.Value)) {
                Cut(id, other);
            }
        }
    }

    public void Connect(int id) {
        Disconnected = Disconnected.Where(x => x.to != id && x.from != id).ToHashSet();
    }

    public void ConnectAll() {
        Disconnected.Clear();
    }

    public bool IsConnected(int id1, int id2) {
        return !Disconnected.Contains(new Connection(id1, id2)) &&
               !Disconnected.Contains(new Connection(id2, id1));
    }

    public Connected Clone() {
        return new Connected(N) {
            Disconnected = new HashSet<Connection>(Disconnected),
        };
    }
}