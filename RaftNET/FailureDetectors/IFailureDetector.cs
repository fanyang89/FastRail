namespace RaftNET.FailureDetectors;

public interface IFailureDetector {
    bool IsAlive(ulong server);
    void AddEndpoint(ulong serverId);
    void RemoveEndpoint(ulong serverId);
}
