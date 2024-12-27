namespace RaftNET;

public interface IFailureDetector {
    bool IsAlive(ulong server);
}