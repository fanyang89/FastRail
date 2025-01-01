using FastRail.Jutes.Data;

namespace FastRail.Jutes.Proto;

internal class GetDataResponse : IJuteDeserializable, IJuteSerializable {
    public byte[]? Data;
    public Stat? Stat;

    public void DeserializeFrom(Stream s) {
        Data = JuteDeserializer.DeserializeBuffer(s);
        Stat = JuteDeserializer.Deserialize<Stat>(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Data);
        JuteSerializer.SerializeTo(s, Stat);
    }
}