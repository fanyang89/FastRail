using FastRail.Jutes.Data;

namespace FastRail.Jutes.Proto;

internal class GetChildren2Response : IJuteDeserializable, IJuteSerializable {
    public IList<string>? Children;
    public Stat? Stat;

    public void DeserializeFrom(Stream s) {
        Children = JuteDeserializer.DeserializeStringList(s);
        Stat = JuteDeserializer.Deserialize<Stat>(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Children);
        JuteSerializer.SerializeTo(s, Stat);
    }
}