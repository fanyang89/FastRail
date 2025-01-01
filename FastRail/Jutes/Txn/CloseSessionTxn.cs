namespace FastRail.Jutes.Txn;

internal class CloseSessionTxn : IJuteDeserializable, IJuteSerializable {
    public IList<string>? Paths2Delete;

    public void DeserializeFrom(Stream s) {
        Paths2Delete = JuteDeserializer.DeserializeStringList(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Paths2Delete);
    }
}