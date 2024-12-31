namespace FastRail.Protos;

public static class ProtobufMessageExtension {
    public static bool IsValidCreateMode(this int flags) {
        return flags is >= 0 and <= 6;
    }

    public static CreateMode ParseCreateMode(this int flags) {
        return flags switch {
            0 => CreateMode.Persistent,
            1 => CreateMode.Ephemeral,
            2 => CreateMode.Sequence,
            3 => CreateMode.EphemeralSequential,
            4 => CreateMode.Container,
            5 => CreateMode.Ttl,
            6 => CreateMode.PersistentSequentialWithTtl,
            _ => throw new ArgumentOutOfRangeException(nameof(flags))
        };
    }
}