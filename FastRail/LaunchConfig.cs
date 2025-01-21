using System.Net;
using RaftNET.Services;

namespace FastRail;

public class LaunchConfig {
    public required AddressBook AddressBook;
    public required string DataDir;
    public required IPEndPoint Listen;
    public required ulong MyId;
    public required string RaftDataDir;
    public required IPEndPoint RaftListen;

    public Server.Server.Config ServerConfig => new() { DataDir = DataDir, EndPoint = Listen };
}
