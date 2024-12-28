namespace RaftNET;

public interface IFailureDetector {
    bool IsAlive(ulong server);
}

public class TrivialFailureDetector : IFailureDetector {
    public bool IsAlive(ulong server) {
        return true;
    }
}