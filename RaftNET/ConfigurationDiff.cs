namespace RaftNET;

public record ConfigurationDiff {
    public ConfigMemberSet Joining = [];
    public ConfigMemberSet Leaving = [];
}
