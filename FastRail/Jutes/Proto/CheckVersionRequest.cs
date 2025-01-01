namespace FastRail.Jutes.Proto;

internal class CheckVersionRequest : IJuteDeserializable, IJuteSerializable {
    public string? Path;
    public int Version;

    public void DeserializeFrom(Stream s) {
        Path = JuteDeserializer.DeserializeString(s);
        Version = JuteDeserializer.DeserializeInt(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Path);
        JuteSerializer.SerializeTo(s, Version);
    }
}