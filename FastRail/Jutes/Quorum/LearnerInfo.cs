namespace FastRail.Jutes.Quorum;

internal class LearnerInfo : IJuteDeserializable, IJuteSerializable {
    public long Serverid;
    public int ProtocolVersion;
    public long ConfigVersion;

    public void DeserializeFrom(Stream s) {
        Serverid = JuteDeserializer.DeserializeLong(s);
        ProtocolVersion = JuteDeserializer.DeserializeInt(s);
        ConfigVersion = JuteDeserializer.DeserializeLong(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Serverid);
        JuteSerializer.SerializeTo(s, ProtocolVersion);
        JuteSerializer.SerializeTo(s, ConfigVersion);
    }
}