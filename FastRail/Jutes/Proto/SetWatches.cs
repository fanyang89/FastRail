﻿namespace FastRail.Jutes.Proto;

internal class SetWatches : IJuteDeserializable, IJuteSerializable {
    public IList<string>? ChildWatches;
    public IList<string>? DataWatches;
    public IList<string>? ExistWatches;
    public long RelativeZxid;

    public void DeserializeFrom(Stream s) {
        RelativeZxid = JuteDeserializer.DeserializeInt(s);
        DataWatches = JuteDeserializer.DeserializeStringList(s);
        ExistWatches = JuteDeserializer.DeserializeStringList(s);
        ChildWatches = JuteDeserializer.DeserializeStringList(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, RelativeZxid);
        JuteSerializer.SerializeTo(s, DataWatches);
        JuteSerializer.SerializeTo(s, ExistWatches);
        JuteSerializer.SerializeTo(s, ChildWatches);
    }
}
