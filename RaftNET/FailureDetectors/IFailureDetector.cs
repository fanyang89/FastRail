namespace RaftNET.FailureDetectors;

public interface IFailureDetector {
    bool IsAlive(ulong server);
}
