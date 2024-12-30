namespace FastRail.Jutes.Proto;

class GetEphemeralsResponse : IJuteDeserializable, IJuteSerializable {
    public IList<string>? Ephemerals;

    public void DeserializeFrom(Stream s) {
        Ephemerals = JuteDeserializer.DeserializeStringList(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Ephemerals);
    }
}