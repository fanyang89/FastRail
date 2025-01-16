using OneOf;

namespace RaftNET.Services;

public class Event : OneOfBase<RoleChangeEvent> {
    protected Event(OneOf<RoleChangeEvent> input) : base(input) {}

    public Event(RoleChangeEvent input) : base(input) {}
}
