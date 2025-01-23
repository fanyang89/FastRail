namespace RaftNET.Tests.ReplicationTests;

public class RpcAddressSet : HashSet<NodeId>, IEquatable<ServerAddressSet> {
    public bool Equals(ServerAddressSet? other) {
        if (ReferenceEquals(null, other)) {
            return false;
        }
        return other.Count == Count && other.All(address => this.Any(x => x.Id == address.ServerId));
    }
}
