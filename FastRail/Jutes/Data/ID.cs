namespace FastRail.Jutes.Data;

public record ID : IJuteDeserializable, IJuteSerializable {
    public string? Scheme;
    public string? Id;

    public void DeserializeFrom(Stream s) {
        Scheme = JuteDeserializer.DeserializeString(s);
        Id = JuteDeserializer.DeserializeString(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Scheme);
        JuteSerializer.SerializeTo(s, Id);
    }
}
