namespace RaftNET.Tests.ReplicationTests;

public class RpcAddressSet : HashSet<NodeId>, IEquatable<ServerAddressSet> {
    public bool Equals(ServerAddressSet? other) {
        return !ReferenceEquals(other, null) && other.Equals(this);
    }
}
