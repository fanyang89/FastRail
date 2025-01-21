using RaftNET.FailureDetectors;

namespace RaftNET.Tests.ReplicationTests;

class MockFailureDetector(ulong id, Connected connected) : IFailureDetector {
    #region IFailureDetector Members

    public bool IsAlive(ulong server) {
        return connected.IsConnected(id, server);
    }

    #endregion
}
