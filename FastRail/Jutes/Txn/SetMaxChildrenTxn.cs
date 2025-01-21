namespace FastRail.Jutes.Txn;

internal class SetMaxChildrenTxn : IJuteDeserializable, IJuteSerializable {
    public int Max;
    public string? Path;

    public void DeserializeFrom(Stream s) {
        Path = JuteDeserializer.DeserializeString(s);
        Max = JuteDeserializer.DeserializeInt(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Path);
        JuteSerializer.SerializeTo(s, Max);
    }
}
