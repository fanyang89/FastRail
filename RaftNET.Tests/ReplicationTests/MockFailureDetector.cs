using RaftNET.FailureDetectors;

namespace RaftNET.Tests.ReplicationTests;

class MockFailureDetector(ulong id, Connected connected) : IFailureDetector {
    public bool IsAlive(ulong server) {
        return connected.IsConnected(id, server);
    }

    public void AddEndpoint(ulong serverId) {
        throw new NotImplementedException();
    }

    public void RemoveEndpoint(ulong serverId) {
        throw new NotImplementedException();
    }
}
