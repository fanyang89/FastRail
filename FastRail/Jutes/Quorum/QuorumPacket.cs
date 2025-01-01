using FastRail.Jutes.Data;

namespace FastRail.Jutes.Quorum;

internal class QuorumPacket : IJuteDeserializable, IJuteSerializable {
    public int Type; // Request, Ack, Commit, Ping
    public long Zxid;
    public byte[]? Data; // Only significant when type is request
    public IList<ID>? Authinfo;

    public void DeserializeFrom(Stream s) {
        Type = JuteDeserializer.DeserializeInt(s);
        Zxid = JuteDeserializer.DeserializeLong(s);
        Data = JuteDeserializer.DeserializeBuffer(s);
        Authinfo = JuteDeserializer.DeserializeList<ID>(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Type);
        JuteSerializer.SerializeTo(s, Zxid);
        JuteSerializer.SerializeTo(s, Data);
        JuteSerializer.SerializeTo(s, Authinfo);
    }
}