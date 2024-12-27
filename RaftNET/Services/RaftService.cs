using Microsoft.Extensions.Logging;

namespace RaftNET.Services;

public class RaftService(ILogger<RaftService> logger) : Raft.RaftBase {
    private readonly ILogger<RaftService> logger_ = logger;
}