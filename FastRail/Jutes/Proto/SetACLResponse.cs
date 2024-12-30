using FastRail.Jutes.Data;

namespace FastRail.Jutes.Proto;

class SetACLResponse : IJuteDeserializable, IJuteSerializable {
    public Stat? Stat;

    public void DeserializeFrom(Stream s) {
        Stat = JuteDeserializer.Deserialize<Stat>(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Stat);
    }
}