﻿namespace FastRail.Jutes.Proto;

public class RequestHeader : IJuteDeserializable, IJuteSerializable {
    public int Type;
    public int Xid;

    public static int SizeOf => sizeof(int) + sizeof(int);

    public void DeserializeFrom(Stream s) {
        Xid = JuteDeserializer.DeserializeInt(s);
        Type = JuteDeserializer.DeserializeInt(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Xid);
        JuteSerializer.SerializeTo(s, Type);
    }
}
