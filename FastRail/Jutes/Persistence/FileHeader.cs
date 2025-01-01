namespace FastRail.Jutes.Persistence;

internal class FileHeader : IJuteDeserializable, IJuteSerializable {
    public int Magic;
    public int Version;
    public long Dbid;

    public void DeserializeFrom(Stream s) {
        Magic = JuteDeserializer.DeserializeInt(s);
        Version = JuteDeserializer.DeserializeInt(s);
        Dbid = JuteDeserializer.DeserializeLong(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Magic);
        JuteSerializer.SerializeTo(s, Version);
        JuteSerializer.SerializeTo(s, Dbid);
    }
}