﻿namespace FastRail.Jutes.Proto;

internal class MultiHeader : IJuteDeserializable, IJuteSerializable {
    public int Type;
    public bool Done;
    public int Err;

    public void DeserializeFrom(Stream s) {
        Type = JuteDeserializer.DeserializeInt(s);
        Done = JuteDeserializer.DeserializeBool(s);
        Err = JuteDeserializer.DeserializeInt(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Type);
        JuteSerializer.SerializeTo(s, Done);
        JuteSerializer.SerializeTo(s, Err);
    }
}