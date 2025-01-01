namespace FastRail.Jutes.Proto;

internal class AddWatchRequest : IJuteDeserializable, IJuteSerializable {
    public string? Path;
    public int Mode;

    public void DeserializeFrom(Stream s) {
        Path = JuteDeserializer.DeserializeString(s);
        Mode = JuteDeserializer.DeserializeInt(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Path);
        JuteSerializer.SerializeTo(s, Mode);
    }
}