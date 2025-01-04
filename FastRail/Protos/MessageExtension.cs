using FastRail.Jutes.Data;

namespace FastRail.Protos;

public static class MessageExtension {
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

    public static Stat ToStat(this StatEntry entry) {
        return new Stat {
            Czxid = entry.Czxid,
            Mzxid = entry.Mzxid,
            Ctime = entry.Ctime,
            Mtime = entry.Mtime,
            Version = entry.Version,
            Cversion = entry.Cversion,
            Aversion = entry.Aversion,
            EphemeralOwner = entry.EphemeralOwner,
            DataLength = entry.DataLength,
            NumChildren = entry.NumChildren,
            Pzxid = entry.Pzxid
        };
    }
}