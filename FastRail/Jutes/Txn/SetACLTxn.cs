using FastRail.Jutes.Data;

namespace FastRail.Jutes.Txn;

internal class SetACLTxn : IJuteDeserializable, IJuteSerializable {
    public IList<ACL>? ACL;
    public string? Path;
    public int Version;

    public void DeserializeFrom(Stream s) {
        Path = JuteDeserializer.DeserializeString(s);
        ACL = JuteDeserializer.DeserializeList<ACL>(s);
        Version = JuteDeserializer.DeserializeInt(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Path);
        JuteSerializer.SerializeTo(s, ACL);
        JuteSerializer.SerializeTo(s, Version);
    }
}
