using FastRail.Jutes.Data;

namespace FastRail.Jutes.Txn;

internal class CreateContainerTxn : IJuteDeserializable, IJuteSerializable {
    public IList<ACL>? ACL;
    public byte[]? Data;
    public int ParentCVersion;
    public string? Path;

    public void DeserializeFrom(Stream s) {
        Path = JuteDeserializer.DeserializeString(s);
        Data = JuteDeserializer.DeserializeBuffer(s);
        ACL = JuteDeserializer.DeserializeList<ACL>(s);
        ParentCVersion = JuteDeserializer.DeserializeInt(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Path);
        JuteSerializer.SerializeTo(s, Data);
        JuteSerializer.SerializeTo(s, ACL);
        JuteSerializer.SerializeTo(s, ParentCVersion);
    }
}
