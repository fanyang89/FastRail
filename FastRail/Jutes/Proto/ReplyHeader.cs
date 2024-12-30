namespace FastRail.Jutes.Proto;

class ReplyHeader : IJuteDeserializable, IJuteSerializable {
    public int Xid;
    public long Zxid;
    public int Err;

    public void DeserializeFrom(Stream s) {
        Xid = JuteDeserializer.DeserializeInt(s);
        Zxid = JuteDeserializer.DeserializeLong(s);
        Err = JuteDeserializer.DeserializeInt(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Xid);
        JuteSerializer.SerializeTo(s, Zxid);
        JuteSerializer.SerializeTo(s, Err);
    }
}