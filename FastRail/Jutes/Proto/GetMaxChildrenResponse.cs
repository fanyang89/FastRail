namespace FastRail.Jutes.Proto;

class GetMaxChildrenResponse : IJuteDeserializable, IJuteSerializable {
    public int Max;

    public void DeserializeFrom(Stream s) {
        Max = JuteDeserializer.DeserializeInt(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Max);
    }
}