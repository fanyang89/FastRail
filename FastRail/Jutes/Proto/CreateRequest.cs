using FastRail.Jutes.Data;

namespace FastRail.Jutes.Proto;

public class CreateRequest : IJuteDeserializable, IJuteSerializable {
    public IList<ACL>? ACL;
    public byte[]? Data;
    public int Flags;
    public string? Path;

    public void DeserializeFrom(Stream s) {
        Path = JuteDeserializer.DeserializeString(s);
        Data = JuteDeserializer.DeserializeBuffer(s);
        ACL = JuteDeserializer.DeserializeList<ACL>(s);
        Flags = JuteDeserializer.DeserializeInt(s);
    }

    public void SerializeTo(Stream s) {
        throw new NotImplementedException();
    }
}
