namespace RaftNET.Tests.Replications;

public class ServerAddressSet : HashSet<ServerAddress>, IEquatable<RpcAddressSet> {
    public bool Equals(RpcAddressSet? other) {
        if (ReferenceEquals(null, other)) {
            return false;
        }
        return other.Count == Count &&
               other.All(address => this.Any(x => x.ServerId == (ulong)address.Id));
    }
}
