namespace FastRail.Jutes.Proto;

public class ConnectRequest : IJuteDeserializable, IJuteSerializable {
    public int ProtocolVersion;
    public long LastZxidSeen;
    public int Timeout;
    public long SessionId;
    public byte[]? Passwd;
    public bool ReadOnly;

    public void DeserializeFrom(Stream s) {
        ProtocolVersion = JuteDeserializer.DeserializeInt(s);
        LastZxidSeen = JuteDeserializer.DeserializeLong(s);
        Timeout = JuteDeserializer.DeserializeInt(s);
        SessionId = JuteDeserializer.DeserializeLong(s);
        Passwd = JuteDeserializer.DeserializeBuffer(s);
        ReadOnly = JuteDeserializer.DeserializeBool(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, ProtocolVersion);
        JuteSerializer.SerializeTo(s, LastZxidSeen);
        JuteSerializer.SerializeTo(s, Timeout);
        JuteSerializer.SerializeTo(s, SessionId);
        JuteSerializer.SerializeTo(s, Passwd);
        JuteSerializer.SerializeTo(s, ReadOnly);
    }
}