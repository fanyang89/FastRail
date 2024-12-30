using FastRail.Jutes.Data;

namespace FastRail.Jutes.Proto;

class GetACLResponse : IJuteDeserializable, IJuteSerializable {
    public IList<ACL>? ACL;
    public Stat? Stat;

    public void DeserializeFrom(Stream s) {
        ACL = JuteDeserializer.DeserializeList<ACL>(s);
        Stat = JuteDeserializer.Deserialize<Stat>(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, ACL);
        JuteSerializer.SerializeTo(s, Stat);
    }
}