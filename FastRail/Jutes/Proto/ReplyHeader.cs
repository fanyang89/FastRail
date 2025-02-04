﻿namespace FastRail.Jutes.Proto;

internal class ReplyHeader(int xid, long zxid, int err = 0) : IJuteDeserializable, IJuteSerializable {
    public int Err = err;
    public int Xid = xid;
    public long Zxid = zxid;

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
