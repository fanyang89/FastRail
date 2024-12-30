namespace FastRail.Jutes.Data;

public record StatPersisted : IJuteDeserializable, IJuteSerializable {
    public long Czxid; // created zxid
    public long Mzxid; // last modified zxid
    public long Ctime; // created
    public long Mtime; // last modified
    public int Version; // version
    public int Cversion; // child version
    public int Aversion; // acl version
    public long EphemeralOwner; // owner id if ephemeral, 0 otw
    public long Pzxid; // last modified children

    public void DeserializeFrom(Stream s) {
        Czxid = JuteDeserializer.DeserializeLong(s);
        Mzxid = JuteDeserializer.DeserializeLong(s);
        Ctime = JuteDeserializer.DeserializeLong(s);
        Mtime = JuteDeserializer.DeserializeLong(s);
        Version = JuteDeserializer.DeserializeInt(s);
        Cversion = JuteDeserializer.DeserializeInt(s);
        Aversion = JuteDeserializer.DeserializeInt(s);
        EphemeralOwner = JuteDeserializer.DeserializeLong(s);
        Pzxid = JuteDeserializer.DeserializeLong(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Czxid);
        JuteSerializer.SerializeTo(s, Mzxid);
        JuteSerializer.SerializeTo(s, Ctime);
        JuteSerializer.SerializeTo(s, Mtime);
        JuteSerializer.SerializeTo(s, Version);
        JuteSerializer.SerializeTo(s, Cversion);
        JuteSerializer.SerializeTo(s, Aversion);
        JuteSerializer.SerializeTo(s, EphemeralOwner);
        JuteSerializer.SerializeTo(s, Pzxid);
    }
}