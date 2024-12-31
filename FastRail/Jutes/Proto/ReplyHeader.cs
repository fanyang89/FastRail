namespace FastRail.Jutes.Proto;

class ReplyHeader(int xid, long zxid, int err = 0) : IJuteDeserializable, IJuteSerializable {
    public int Xid = xid;
    public long Zxid = zxid;
    public int Err = err;

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