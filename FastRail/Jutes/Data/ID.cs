﻿namespace FastRail.Jutes.Data;

public record ID : IJuteDeserializable, IJuteSerializable {
    public string? Id;
    public string? Scheme;

    public void DeserializeFrom(Stream s) {
        Scheme = JuteDeserializer.DeserializeString(s);
        Id = JuteDeserializer.DeserializeString(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Scheme);
        JuteSerializer.SerializeTo(s, Id);
    }
}
