namespace FastRail.Jutes.Data;

public record StatPersisted : IJuteDeserializable, IJuteSerializable {
    // created zxid
    public long CZxid;

    // last modified zxid
    public long MZxid;

    // created
    public long CTime;

    // last modified
    public long MTime;

    // version
    public int Version;

    // child version
    public int CVersion;

    // acl version
    public int AVersion;

    // owner id if ephemeral, 0 otw
    public long EphemeralOwner;

    // last modified children
    public long PZxid;

    public void DeserializeFrom(Stream s) {
        CZxid = JuteDeserializer.DeserializeLong(s);
        MZxid = JuteDeserializer.DeserializeLong(s);
        CTime = JuteDeserializer.DeserializeLong(s);
        MTime = JuteDeserializer.DeserializeLong(s);
        Version = JuteDeserializer.DeserializeInt(s);
        CVersion = JuteDeserializer.DeserializeInt(s);
        AVersion = JuteDeserializer.DeserializeInt(s);
        EphemeralOwner = JuteDeserializer.DeserializeLong(s);
        PZxid = JuteDeserializer.DeserializeLong(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, CZxid);
        JuteSerializer.SerializeTo(s, MZxid);
        JuteSerializer.SerializeTo(s, CTime);
        JuteSerializer.SerializeTo(s, MTime);
        JuteSerializer.SerializeTo(s, Version);
        JuteSerializer.SerializeTo(s, CVersion);
        JuteSerializer.SerializeTo(s, AVersion);
        JuteSerializer.SerializeTo(s, EphemeralOwner);
        JuteSerializer.SerializeTo(s, PZxid);
    }

    public Stat ToStat(int dataLength, int numChildren) {
        var s = new Stat {
            Czxid = CZxid,
            Mzxid = MZxid,
            Aversion = AVersion,
            EphemeralOwner = EphemeralOwner,
            DataLength = dataLength,
            NumChildren = numChildren,
            Pzxid = PZxid,
            Ctime = CTime,
            Mtime = MTime,
            Version = Version,
            Cversion = CVersion
        };
        return s;
    }
}