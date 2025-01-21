namespace RaftNET.Tests.ReplicationTests;

public class ServerAddressSet : HashSet<ServerAddress>, IEquatable<RpcAddressSet> {
    #region IEquatable<RpcAddressSet> Members

    public bool Equals(RpcAddressSet? other) {
        if (ReferenceEquals(null, other)) {
            return false;
        }
        return other.Count == Count &&
               other.All(address => this.Any(x => x.ServerId == (ulong)address.Id));
    }

    #endregion
}
