namespace RaftNET.Services;

public class RoleChangeEvent {
    public Role Role { get; set; }
    public ulong ServerId { get; set; }
}