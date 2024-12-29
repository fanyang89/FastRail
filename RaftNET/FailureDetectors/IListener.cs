namespace RaftNET;

public interface IListener {
    void MarkAlive(ulong server);
    void MarkDead(ulong server);
}