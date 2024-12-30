namespace FastRail.Jutes.Proto;

class AuthPacket : IJuteDeserializable, IJuteSerializable {
    public int Type;
    public string? Scheme;
    public byte[]? Auth;

    public void DeserializeFrom(Stream s) {
        Type = JuteDeserializer.DeserializeInt(s);
        Scheme = JuteDeserializer.DeserializeString(s);
        Auth = JuteDeserializer.DeserializeBuffer(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Type);
        JuteSerializer.SerializeTo(s, Scheme);
        JuteSerializer.SerializeTo(s, Auth);
    }
}