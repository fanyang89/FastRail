using RaftNET.Tests.ReplicationTests;

namespace RaftNET.Tests.Exceptions;

class UnknownNodeException(ulong id) : ReplicationTestException {
    public override string Message => $"Unknown node, id={id}";
}
