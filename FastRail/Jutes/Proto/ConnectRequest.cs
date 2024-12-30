namespace FastRail.Jutes.Proto;

class ConnectRequest : IJuteDeserializable, IJuteSerializable {
    public int ProtocolVersion;
    public long LastZxidSeen;
    public int TimeOut;
    public long SessionId;
    public byte[]? Passwd;
    public bool ReadOnly;

    public void DeserializeFrom(Stream s) {
        ProtocolVersion = JuteDeserializer.DeserializeInt(s);
        LastZxidSeen = JuteDeserializer.DeserializeLong(s);
        TimeOut = JuteDeserializer.DeserializeInt(s);
        SessionId = JuteDeserializer.DeserializeLong(s);
        Passwd = JuteDeserializer.DeserializeBuffer(s);
        ReadOnly = JuteDeserializer.DeserializeBool(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, ProtocolVersion);
        JuteSerializer.SerializeTo(s, LastZxidSeen);
        JuteSerializer.SerializeTo(s, TimeOut);
        JuteSerializer.SerializeTo(s, SessionId);
        JuteSerializer.SerializeTo(s, Passwd);
        JuteSerializer.SerializeTo(s, ReadOnly);
    }
}