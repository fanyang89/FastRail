namespace RaftNET.FailureDetectors;

public interface IListener {
    void MarkAlive(ulong server);
    void MarkDead(ulong server);
}
