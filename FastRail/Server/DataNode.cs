using FastRail.Jutes;
using FastRail.Jutes.Data;

namespace FastRail.Server;

public class DataNode : IJuteSerializable, IJuteDeserializable {
    public byte[] Data;
    public long ACL;
    public StatPersisted Stat;

    public DataNode(byte[] data, long acl, StatPersisted stat) {
        Data = data;
        ACL = acl;
        Stat = stat;
    }

    public DataNode() {
        Data = [];
        ACL = 0;
        Stat = new StatPersisted();
    }

    public DataNode(StatPersisted stat) {
        Data = [];
        ACL = 0;
        Stat = stat;
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Data);
        JuteSerializer.SerializeTo(s, ACL);
        JuteSerializer.SerializeTo(s, Stat);
    }

    public void DeserializeFrom(Stream s) {
        Data = JuteDeserializer.DeserializeBuffer(s);
        ACL = JuteDeserializer.DeserializeLong(s);
        Stat = JuteDeserializer.Deserialize<StatPersisted>(s);
    }
}