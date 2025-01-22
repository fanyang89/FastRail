using RaftNET.FailureDetectors;

namespace RaftNET.Services;

public class ConnectionManagerProxy(ConnectionManager connectionManager, IFailureDetector failureDetector) : IRaftRpcClient {
    public Task AppendRequestAsync(ulong to, AppendRequest request) {
        return connectionManager.AppendRequestAsync(to, request);
    }

    public Task AppendResponseAsync(ulong to, AppendResponse request) {
        return connectionManager.AppendResponseAsync(to, request);
    }

    public Task PingAsync(ulong to, DateTime deadline, CancellationToken cancellationToken) {
        return connectionManager.PingAsync(to, deadline, cancellationToken);
    }

    public Task ReadQuorumRequestAsync(ulong to, ReadQuorumRequest request) {
        return connectionManager.ReadQuorumRequestAsync(to, request);
    }

    public Task ReadQuorumResponseAsync(ulong to, ReadQuorumResponse response) {
        return connectionManager.ReadQuorumResponseAsync(to, response);
    }

    public Task<SnapshotResponse> SendSnapshotAsync(ulong to, InstallSnapshotRequest request) {
        return connectionManager.SendSnapshotAsync(to, request);
    }

    public Task TimeoutNowRequestAsync(ulong to, TimeoutNowRequest request) {
        return connectionManager.TimeoutNowRequestAsync(to, request);
    }

    public Task VoteRequestAsync(ulong to, VoteRequest request) {
        return connectionManager.VoteRequestAsync(to, request);
    }

    public Task VoteResponseAsync(ulong to, VoteResponse response) {
        return connectionManager.VoteResponseAsync(to, response);
    }

    public void OnConfigurationChange(ISet<ServerAddress> add, ISet<ServerAddress> del) {
        foreach (var address in add) {
            if (address.ServerId != connectionManager.Id) {
                failureDetector.AddEndpoint(address.ServerId);
            }
        }
        foreach (var address in del) {
            failureDetector.RemoveEndpoint(address.ServerId);
        }
    }
}
