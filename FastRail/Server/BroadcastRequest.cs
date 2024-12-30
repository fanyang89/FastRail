using CommunityToolkit.HighPerformance;
using FastRail.Jutes;
using FastRail.Jutes.Proto;

namespace FastRail.Server;

public record BroadcastRequest : IJuteDeserializable, IJuteSerializable {
    public RequestHeader? Header;
    public byte[]? Body;

    public BroadcastRequest(RequestHeader Header, byte[] Body) {
        this.Header = Header;
        this.Body = Body;
    }

    public BroadcastRequest(ReadOnlyMemory<byte> data) {
        DeserializeFrom(data.AsStream());
    }

    public void SerializeTo(Stream s) {
        JuteSerializer.SerializeTo(s, Header);
        JuteSerializer.SerializeTo(s, Body);
    }

    public OpCode? Type => Header?.Type.ToEnum();

    public void DeserializeFrom(Stream s) {
        Header = JuteDeserializer.Deserialize<RequestHeader>(s);
        var length = JuteDeserializer.DeserializeInt(s);
        var bodyBuffer = new byte[length];
        s.ReadExactly(bodyBuffer, 0, length);
        Body = bodyBuffer;
    }
}