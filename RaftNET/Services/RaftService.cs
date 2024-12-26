using Grpc.Core;
using RaftNET;

namespace RaftNET.Services;

// public class GreeterService : 
// {
//     private readonly ILogger<GreeterService> _logger;
//     public GreeterService(ILogger<GreeterService> logger)
//     {
//         _logger = logger;
//     }
//
//     public override Task<HelloReply> SayHello(HelloRequest request, ServerCallContext context)
//     {
//         return Task.FromResult(new HelloReply
//         {
//             Message = "Hello " + request.Name
//         });
//     }
// }
public class RaftService(ILogger<RaftService> logger) : Raft.RaftBase {
    private readonly ILogger<RaftService> _logger = logger;
}