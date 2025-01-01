namespace FastRail.Jutes.Proto;

internal class GetEphemeralsRequest : IJuteDeserializable, IJuteSerializable {
    public string? PrefixPath;

    public void DeserializeFrom(Stream s) {
        PrefixPath = JuteDeserializer.DeserializeString(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, PrefixPath);
    }
}