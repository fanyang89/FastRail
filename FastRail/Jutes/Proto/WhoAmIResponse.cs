using FastRail.Jutes.Data;

namespace FastRail.Jutes.Proto;

internal class WhoAmIResponse : IJuteDeserializable, IJuteSerializable {
    public IList<ClientInfo>? ClientInfo;

    public void DeserializeFrom(Stream s) {
        ClientInfo = JuteDeserializer.DeserializeList<ClientInfo>(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, ClientInfo);
    }
}