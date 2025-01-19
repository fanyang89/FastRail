namespace RaftNET.Tests.ReplicationTests;

public class RpcConfig {
    public bool Drops = false;
    public TimeSpan NetworkDelay = TimeSpan.Zero;
    public TimeSpan LocalDelay = TimeSpan.Zero;
    public int LocalNodes = 32;
    public TimeSpan ExtraDelayMax = TimeSpan.FromMicroseconds(500);
}
