namespace FastRail.Jutes.Proto;

public class ConnectResponse : IJuteDeserializable, IJuteSerializable {
    public byte[]? Passwd;
    public int ProtocolVersion;
    public bool ReadOnly;
    public long SessionId;
    public int Timeout;

    public void DeserializeFrom(Stream s) {
        ProtocolVersion = JuteDeserializer.DeserializeInt(s);
        Timeout = JuteDeserializer.DeserializeInt(s);
        SessionId = JuteDeserializer.DeserializeLong(s);
        Passwd = JuteDeserializer.DeserializeBuffer(s);
        ReadOnly = JuteDeserializer.DeserializeBool(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, ProtocolVersion);
        JuteSerializer.SerializeTo(s, Timeout);
        JuteSerializer.SerializeTo(s, SessionId);
        JuteSerializer.SerializeTo(s, Passwd);
        JuteSerializer.SerializeTo(s, ReadOnly);
    }
}
