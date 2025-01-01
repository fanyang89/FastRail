namespace FastRail.Jutes.Txn;

internal class SetMaxChildrenTxn : IJuteDeserializable, IJuteSerializable {
    public string? Path;
    public int Max;

    public void DeserializeFrom(Stream s) {
        Path = JuteDeserializer.DeserializeString(s);
        Max = JuteDeserializer.DeserializeInt(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Path);
        JuteSerializer.SerializeTo(s, Max);
    }
}