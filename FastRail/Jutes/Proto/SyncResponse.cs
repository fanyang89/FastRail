namespace FastRail.Jutes.Proto;

class SyncResponse : IJuteDeserializable, IJuteSerializable {
    public string? Path;

    public void DeserializeFrom(Stream s) {
        Path = JuteDeserializer.DeserializeString(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Path);
    }
}