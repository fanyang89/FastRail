﻿namespace FastRail.Jutes.Proto;

internal class SetSaslResponse : IJuteDeserializable, IJuteSerializable {
    public byte[]? Token;

    public void DeserializeFrom(Stream s) {
        Token = JuteDeserializer.DeserializeBuffer(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Token);
    }
}
