using RaftNET.Services;

namespace RaftNET.Tests.ReplicationTests;

public class RpcNet : Dictionary<ulong, (MockRpc, IRaftRpcHandler)>;
