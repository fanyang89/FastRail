namespace FastRail.Jutes.Txn;

internal class MultiTxn : IJuteDeserializable, IJuteSerializable {
    public IList<Txn>? Txns;

    public void DeserializeFrom(Stream s) {
        Txns = JuteDeserializer.DeserializeList<Txn>(s);
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Txns);
    }
}
