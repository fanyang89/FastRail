using System.Net;
using RaftNET.Services;

namespace FastRail;

public class LaunchConfig {
    public required ulong MyId;
    public required string DataDir;
    public required IPEndPoint Listen;
    public required IPEndPoint RaftListen;
    public required string RaftDataDir;
    public required AddressBook AddressBook;

    public Server.Server.Config ServerConfig => new() { DataDir = DataDir, EndPoint = Listen };

    public RaftServiceConfig GetRaftConfig(Server.Server server) {
        return new RaftServiceConfig {
            MyId = MyId,
            DataDir = RaftDataDir,
            LoggerFactory = LoggerFactory.Instance,
            StateMachine = server,
            AddressBook = AddressBook,
            Listen = RaftListen
        };
    }
}
