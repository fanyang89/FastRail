namespace RaftNET.FailureDetectors;

public class TrivialFailureDetector : IFailureDetector {
    public bool IsAlive(ulong server) {
        return true;
    }

    public void AddEndpoint(ulong serverId) {}

    public void RemoveEndpoint(ulong serverId) {}
}
