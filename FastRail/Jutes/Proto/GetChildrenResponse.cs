namespace FastRail.Jutes.Proto;

class GetChildrenResponse : IJuteDeserializable, IJuteSerializable {
    public IList<string>? Children;

    public void DeserializeFrom(Stream s) {
        Children = JuteDeserializer.DeserializeStringList(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Children);
    }
}