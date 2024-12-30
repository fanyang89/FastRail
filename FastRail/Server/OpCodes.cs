namespace FastRail.Server;

static class OpCodes {
    public static OpCode? ToEnum(this int code) {
        if (Enum.IsDefined(typeof(OpCode), code)) {
            return (OpCode)code;
        }
        return null;
    }
}