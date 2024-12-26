using RaftNET.Services;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddGrpc();

var app = builder.Build();
app.MapGrpcService<RaftService>();
app.MapGet("/", () => "Raft gRPC service");
app.Run();