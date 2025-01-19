using FastRail.Protos;

namespace FastRail.Server;

public record InMemorySession(
    SessionEntry Session,
    List<string> Ephemerals
);