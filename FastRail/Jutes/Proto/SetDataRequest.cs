namespace FastRail.Jutes.Proto;

public class SetDataRequest : IJuteDeserializable, IJuteSerializable {
    public byte[]? Data;
    public string? Path;
    public int Version;

    public void DeserializeFrom(Stream s) {
        Path = JuteDeserializer.DeserializeString(s);
        Data = JuteDeserializer.DeserializeBuffer(s);
        Version = JuteDeserializer.DeserializeInt(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Path);
        JuteSerializer.SerializeTo(s, Data);
        JuteSerializer.SerializeTo(s, Version);
    }
}
