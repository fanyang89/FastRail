namespace FastRail.Jutes.Quorum;

internal class QuorumAuthPacket : IJuteDeserializable, IJuteSerializable {
    public long Magic;
    public int Status;
    public byte[]? Token;

    public void DeserializeFrom(Stream s) {
        Magic = JuteDeserializer.DeserializeLong(s);
        Status = JuteDeserializer.DeserializeInt(s);
        Token = JuteDeserializer.DeserializeBuffer(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Magic);
        JuteSerializer.SerializeTo(s, Status);
        JuteSerializer.SerializeTo(s, Token);
    }
}