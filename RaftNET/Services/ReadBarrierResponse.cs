using OneOf;
using RaftNET.Exceptions;

namespace RaftNET.Services;

class ReadBarrierResponse : OneOfBase<MonoState, ulong, NotLeaderException> {
    public ReadBarrierResponse() : base(new MonoState()) {}
    public ReadBarrierResponse(ulong index) : base(index) {}
    public ReadBarrierResponse(NotLeaderException ex) : base(ex) {}
}