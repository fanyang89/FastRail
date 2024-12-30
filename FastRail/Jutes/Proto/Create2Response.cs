using FastRail.Jutes.Data;

namespace FastRail.Jutes.Proto;

class Create2Response : IJuteDeserializable, IJuteSerializable {
    public string? Path;
    public Stat? Stat;

    public void DeserializeFrom(Stream s) {
        Path = JuteDeserializer.DeserializeString(s);
        Stat = JuteDeserializer.Deserialize<Stat>(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Path);
        JuteSerializer.SerializeTo(s, Stat);
    }
}