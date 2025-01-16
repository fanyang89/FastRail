using FastRail.Jutes.Data;

namespace FastRail.Jutes.Txn;

internal class CreateTxn : IJuteDeserializable, IJuteSerializable {
    public string? Path;
    public byte[]? Data;
    public IList<ACL>? ACL;
    public bool Ephemeral;
    public int ParentCVersion;

    public void DeserializeFrom(Stream s) {
        Path = JuteDeserializer.DeserializeString(s);
        Data = JuteDeserializer.DeserializeBuffer(s);
        ACL = JuteDeserializer.DeserializeList<ACL>(s);
        Ephemeral = JuteDeserializer.DeserializeBool(s);
        ParentCVersion = JuteDeserializer.DeserializeInt(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Path);
        JuteSerializer.SerializeTo(s, Data);
        JuteSerializer.SerializeTo(s, ACL);
        JuteSerializer.SerializeTo(s, Ephemeral);
        JuteSerializer.SerializeTo(s, ParentCVersion);
    }
}
