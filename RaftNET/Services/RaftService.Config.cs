using System.Net;
using Microsoft.Extensions.Logging;
using RaftNET.StateMachines;

namespace RaftNET.Services;

public partial class RaftService {
    public record Config(
        ulong MyId,
        string DataDir,
        ILoggerFactory LoggerFactory,
        IStateMachine StateMachine,
        AddressBook AddressBook,
        IPAddress ListenAddress,
        int Port,
        int PingInterval = 1000, // ms
        int PingTimeout = 1000, // ms
        int AppendRequestThreshold = 100000,
        bool EnablePreVote = true,
        int MaxLogSize = 4 * 1024 * 1024
    );
}