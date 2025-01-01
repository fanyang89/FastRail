namespace FastRail.Jutes.Proto;

internal class ErrorResponse : IJuteDeserializable, IJuteSerializable {
    public int Err;

    public void DeserializeFrom(Stream s) {
        Err = JuteDeserializer.DeserializeInt(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Err);
    }
}