namespace FastRail.Jutes.Proto;

class SetWatches2 : IJuteDeserializable, IJuteSerializable {
    public long RelativeZxid;
    public IList<string>? DataWatches;
    public IList<string>? ExistWatches;
    public IList<string>? ChildWatches;
    public IList<string>? PersistentWatches;
    public IList<string>? PersistentRecursiveWatches;

    public void DeserializeFrom(Stream s) {
        RelativeZxid = JuteDeserializer.DeserializeInt(s);
        DataWatches = JuteDeserializer.DeserializeStringList(s);
        ExistWatches = JuteDeserializer.DeserializeStringList(s);
        ChildWatches = JuteDeserializer.DeserializeStringList(s);
        PersistentWatches = JuteDeserializer.DeserializeStringList(s);
        PersistentRecursiveWatches = JuteDeserializer.DeserializeStringList(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, RelativeZxid);
        JuteSerializer.SerializeTo(s, DataWatches);
        JuteSerializer.SerializeTo(s, ExistWatches);
        JuteSerializer.SerializeTo(s, ChildWatches);
        JuteSerializer.SerializeTo(s, PersistentWatches);
        JuteSerializer.SerializeTo(s, PersistentRecursiveWatches);
    }
}