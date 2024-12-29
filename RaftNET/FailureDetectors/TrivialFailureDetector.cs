namespace RaftNET.FailureDetectors;

public class TrivialFailureDetector : IFailureDetector {
    public bool IsAlive(ulong server) {
        return true;
    }
}