namespace FastRail.Jutes.Txn;

internal class TxnDigest : IJuteDeserializable, IJuteSerializable {
    public int Version;
    public long TreeDigest;

    public void DeserializeFrom(Stream s) {
        Version = JuteDeserializer.DeserializeInt(s);
        TreeDigest = JuteDeserializer.DeserializeLong(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Version);
        JuteSerializer.SerializeTo(s, TreeDigest);
    }
}
