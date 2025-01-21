namespace FastRail.Jutes.Txn;

internal class TxnHeader : IJuteDeserializable, IJuteSerializable {
    public long ClientId;
    public int CXid;
    public long Time;
    public int Type;
    public long ZXid;

    public void DeserializeFrom(Stream s) {
        ClientId = JuteDeserializer.DeserializeLong(s);
        CXid = JuteDeserializer.DeserializeInt(s);
        ZXid = JuteDeserializer.DeserializeLong(s);
        Time = JuteDeserializer.DeserializeLong(s);
        Type = JuteDeserializer.DeserializeInt(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, ClientId);
        JuteSerializer.SerializeTo(s, CXid);
        JuteSerializer.SerializeTo(s, ZXid);
        JuteSerializer.SerializeTo(s, Time);
        JuteSerializer.SerializeTo(s, Type);
    }
}
