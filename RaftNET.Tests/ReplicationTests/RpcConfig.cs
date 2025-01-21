namespace RaftNET.Tests.ReplicationTests;

public class RpcConfig {
    public bool Drops = false;
    public TimeSpan ExtraDelayMax = TimeSpan.FromMicroseconds(500);
    public TimeSpan LocalDelay = TimeSpan.Zero;
    public uint LocalNodes = 32;
    public TimeSpan NetworkDelay = TimeSpan.Zero;
}
