using System.Net;
using Microsoft.Extensions.Logging;
using RaftNET.StateMachines;

namespace RaftNET.Services;

public record RaftServiceConfig {
    public required ulong MyId;
    public required string DataDir;
    public required ILoggerFactory LoggerFactory;
    public required IStateMachine StateMachine;
    public required AddressBook AddressBook;
    public required IPEndPoint Listen;
    public RaftServiceOptions ServerOptions { get; init; } = new();
}