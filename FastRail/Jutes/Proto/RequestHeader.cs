namespace FastRail.Jutes.Proto;

class RequestHeader : IJuteDeserializable, IJuteSerializable {
    public int Xid;
    public int Type;

    public void DeserializeFrom(Stream s) {
        Xid = JuteDeserializer.DeserializeInt(s);
        Type = JuteDeserializer.DeserializeInt(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Xid);
        JuteSerializer.SerializeTo(s, Type);
    }
}