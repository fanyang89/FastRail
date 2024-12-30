﻿namespace FastRail.Jutes.Proto;

class GetSaslRequest : IJuteDeserializable, IJuteSerializable {
    public byte[]? Token;

    public void DeserializeFrom(Stream s) {
        Token = JuteDeserializer.DeserializeBuffer(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Token);
    }
}