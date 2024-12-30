namespace FastRail.Jutes.Data;

public record ACL : IJuteDeserializable, IJuteSerializable {
    public int Perms;
    public ID? Id;

    public void DeserializeFrom(Stream s) {
        Perms = JuteDeserializer.DeserializeInt(s);
        Id = JuteDeserializer.Deserialize<ID>(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Perms);
    }
}