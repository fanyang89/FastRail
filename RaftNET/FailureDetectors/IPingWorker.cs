namespace RaftNET.FailureDetectors;

public interface IPingWorker {
    void Start();
    void Stop();
    void UpdateAddress();
}