namespace RaftNET.Services;

record RpcConfigDiff {
    public ISet<ServerAddress> Joining { get; } = new HashSet<ServerAddress>();
    public ISet<ServerAddress> Leaving { get; } = new HashSet<ServerAddress>();
}
