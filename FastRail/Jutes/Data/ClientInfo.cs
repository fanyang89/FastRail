namespace FastRail.Jutes.Data;

class ClientInfo : IJuteDeserializable, IJuteSerializable {
    public string? AuthScheme; // Authentication scheme
    public string? User; // username or any other id (for example ip)

    public void DeserializeFrom(Stream s) {
        AuthScheme = JuteDeserializer.DeserializeString(s);
        User = JuteDeserializer.DeserializeString(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, AuthScheme);
        JuteSerializer.SerializeTo(s, User);
    }
}