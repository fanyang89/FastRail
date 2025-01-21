namespace FastRail.Jutes.Data;

public record ACL : IJuteDeserializable, IJuteSerializable {
    public ID? Id;
    public int Perms;

    public void DeserializeFrom(Stream s) {
        Perms = JuteDeserializer.DeserializeInt(s);
        Id = JuteDeserializer.Deserialize<ID>(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Perms);
        JuteSerializer.SerializeTo(s, Id);
    }
}
