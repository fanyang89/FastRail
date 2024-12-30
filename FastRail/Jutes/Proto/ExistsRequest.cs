namespace FastRail.Jutes.Proto;

public class ExistsRequest : IJuteDeserializable, IJuteSerializable {
    public string? Path;
    public bool Watch;

    public void DeserializeFrom(Stream s) {
        Path = JuteDeserializer.DeserializeString(s);
        Watch = JuteDeserializer.DeserializeBool(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Path);
        JuteSerializer.SerializeTo(s, Watch);
    }
}