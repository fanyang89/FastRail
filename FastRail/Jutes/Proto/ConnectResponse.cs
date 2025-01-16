namespace FastRail.Jutes.Proto;

public class ConnectResponse : IJuteDeserializable, IJuteSerializable {
    public int ProtocolVersion;
    public int Timeout;
    public long SessionId;
    public byte[]? Passwd;
    public bool ReadOnly;

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
