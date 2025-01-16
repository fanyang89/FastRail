using FastRail.Jutes.Data;

namespace FastRail.Jutes.Proto;

public class SetACLRequest : IJuteDeserializable, IJuteSerializable {
    public string? Path;
    public IList<ACL>? ACL;
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
