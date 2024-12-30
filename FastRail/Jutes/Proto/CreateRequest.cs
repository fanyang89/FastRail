using FastRail.Jutes.Data;

namespace FastRail.Jutes.Proto;

public class CreateRequest : IJuteDeserializable, IJuteSerializable {
    public string? Path;
    public byte[]? Data;
    public IList<ACL>? ACL;
    public int Flags;

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