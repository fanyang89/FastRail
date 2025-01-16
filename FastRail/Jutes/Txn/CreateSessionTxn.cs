namespace FastRail.Jutes.Txn;

internal class CreateSessionTxn : IJuteDeserializable, IJuteSerializable {
    public int TimeOut;

    public void DeserializeFrom(Stream s) {
        TimeOut = JuteDeserializer.DeserializeInt(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, TimeOut);
    }
}
