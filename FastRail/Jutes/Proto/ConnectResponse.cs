namespace FastRail.Jutes.Proto;

class ConnectResponse : IJuteDeserializable, IJuteSerializable {
    public int ProtocolVersion;
    public int TimeOut;
    public long SessionId;
    public byte[]? Passwd;
    public bool ReadOnly;

    public void DeserializeFrom(Stream s) {
        ProtocolVersion = JuteDeserializer.DeserializeInt(s);
        TimeOut = JuteDeserializer.DeserializeInt(s);
        SessionId = JuteDeserializer.DeserializeLong(s);
        Passwd = JuteDeserializer.DeserializeBuffer(s);
        ReadOnly = JuteDeserializer.DeserializeBool(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, ProtocolVersion);
        JuteSerializer.SerializeTo(s, TimeOut);
        JuteSerializer.SerializeTo(s, SessionId);
        JuteSerializer.SerializeTo(s, Passwd);
        JuteSerializer.SerializeTo(s, ReadOnly);
    }
}