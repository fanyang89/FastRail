﻿using FastRail.Jutes.Data;

namespace FastRail.Jutes.Proto;

internal class CreateTtlRequest : IJuteDeserializable, IJuteSerializable {
    public IList<ACL>? ACL;
    public byte[]? Data;
    public int Flags;
    public string? Path;
    public long Ttl;

    public void DeserializeFrom(Stream s) {
        Path = JuteDeserializer.DeserializeString(s);
        Data = JuteDeserializer.DeserializeBuffer(s);
        ACL = JuteDeserializer.DeserializeList<ACL>(s);
        Flags = JuteDeserializer.DeserializeInt(s);
        Ttl = JuteDeserializer.DeserializeLong(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Path);
        JuteSerializer.SerializeTo(s, Data);
        JuteSerializer.SerializeTo(s, ACL);
        JuteSerializer.SerializeTo(s, Flags);
        JuteSerializer.SerializeTo(s, Ttl);
    }
}
