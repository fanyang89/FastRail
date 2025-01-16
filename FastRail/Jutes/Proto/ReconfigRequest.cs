namespace FastRail.Jutes.Proto;

internal class ReconfigRequest : IJuteDeserializable, IJuteSerializable {
    public string? JoiningServers;
    public string? LeavingServers;
    public string? NewMembers;
    public long CurConfigId;

    public void DeserializeFrom(Stream s) {
        JoiningServers = JuteDeserializer.DeserializeString(s);
        LeavingServers = JuteDeserializer.DeserializeString(s);
        NewMembers = JuteDeserializer.DeserializeString(s);
        CurConfigId = JuteDeserializer.DeserializeLong(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, JoiningServers);
        JuteSerializer.SerializeTo(s, LeavingServers);
        JuteSerializer.SerializeTo(s, NewMembers);
        JuteSerializer.SerializeTo(s, CurConfigId);
    }
}
