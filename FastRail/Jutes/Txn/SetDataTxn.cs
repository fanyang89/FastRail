namespace FastRail.Jutes.Txn;

internal class SetDataTxn : IJuteDeserializable, IJuteSerializable {
    public string? Path;
    public byte[]? Data;
    public int Version;

    public void DeserializeFrom(Stream s) {
        Path = JuteDeserializer.DeserializeString(s);
        Data = JuteDeserializer.DeserializeBuffer(s);
        Version = JuteDeserializer.DeserializeInt(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Path);
        JuteSerializer.SerializeTo(s, Data);
        JuteSerializer.SerializeTo(s, Version);
    }
}