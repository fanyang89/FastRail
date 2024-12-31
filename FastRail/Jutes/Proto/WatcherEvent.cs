namespace FastRail.Jutes.Proto;

class WatcherEvent : IJuteDeserializable, IJuteSerializable {
    public int Type; // event type
    public int State; // state of the Keeper client runtime
    public string? Path;

    public void DeserializeFrom(Stream s) {
        Type = JuteDeserializer.DeserializeInt(s);
        State = JuteDeserializer.DeserializeInt(s);
        Path = JuteDeserializer.DeserializeString(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Type);
        JuteSerializer.SerializeTo(s, State);
        JuteSerializer.SerializeTo(s, Path);
    }
}