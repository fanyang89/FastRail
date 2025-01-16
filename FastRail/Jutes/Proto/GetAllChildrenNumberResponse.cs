namespace FastRail.Jutes.Proto;

internal class GetAllChildrenNumberResponse : IJuteDeserializable, IJuteSerializable {
    public int TotalNumber;

    public void DeserializeFrom(Stream s) {
        TotalNumber = JuteDeserializer.DeserializeInt(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, TotalNumber);
    }
}
