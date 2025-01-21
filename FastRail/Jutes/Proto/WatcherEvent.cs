namespace FastRail.Jutes.Proto;

internal class WatcherEvent : IJuteDeserializable, IJuteSerializable {
    public string? Path;
    public int State; // state of the Keeper client runtime
    public int Type; // event type

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
