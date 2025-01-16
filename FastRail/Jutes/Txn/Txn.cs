namespace FastRail.Jutes.Txn;

internal class Txn : IJuteDeserializable, IJuteSerializable {
    public int Type;
    public byte[]? Data;

    public void DeserializeFrom(Stream s) {
        Type = JuteDeserializer.DeserializeInt(s);
        Data = JuteDeserializer.DeserializeBuffer(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Type);
        JuteSerializer.SerializeTo(s, Data);
    }
}
