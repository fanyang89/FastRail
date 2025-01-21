namespace FastRail.Jutes.Proto;

internal class AuthPacket : IJuteDeserializable, IJuteSerializable {
    public byte[]? Auth;
    public string? Scheme;
    public int Type;

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
