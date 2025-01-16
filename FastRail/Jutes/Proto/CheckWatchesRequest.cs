namespace FastRail.Jutes.Proto;

internal class CheckWatchesRequest : IJuteDeserializable, IJuteSerializable {
    public string? Path;
    public int Type;

    public void DeserializeFrom(Stream s) {
        Path = JuteDeserializer.DeserializeString(s);
        Type = JuteDeserializer.DeserializeInt(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Path);
        JuteSerializer.SerializeTo(s, Type);
    }
}
