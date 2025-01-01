using System.Buffers.Binary;
using System.Diagnostics;
using System.Text;

namespace FastRail.Jutes;

public static class JuteDeserializer {
    public static bool DeserializeBool(Stream s) {
        var value = s.ReadByte();
        return value != 0;
    }

    public static byte DeserializeByte(Stream s) {
        return (byte)s.ReadByte();
    }

    public static int DeserializeInt(Stream s) {
        var buf = new byte[sizeof(int)];
        var n = s.Read(buf, 0, buf.Length);
        Debug.Assert(n == buf.Length);
        return BinaryPrimitives.ReadInt32BigEndian(buf);
    }

    public static long DeserializeLong(Stream s) {
        var buf = new byte[sizeof(long)];
        var n = s.Read(buf, 0, buf.Length);
        Debug.Assert(n == buf.Length);
        return BinaryPrimitives.ReadInt64BigEndian(buf);
    }

    public static float DeserializeFloat(Stream s) {
        var buf = new byte[sizeof(float)];
        var n = s.Read(buf, 0, buf.Length);
        Debug.Assert(n == buf.Length);
        return BinaryPrimitives.ReadSingleBigEndian(buf);
    }

    public static double DeserializeDouble(Stream s) {
        var buf = new byte[sizeof(double)];
        var n = s.Read(buf, 0, buf.Length);
        Debug.Assert(n == buf.Length);
        return BinaryPrimitives.ReadDoubleBigEndian(buf);
    }

    public static byte[] DeserializeBuffer(Stream s) {
        var len = DeserializeInt(s);
        var buf = new byte[len];
        var n = s.Read(buf, 0, buf.Length);
        Debug.Assert(n == buf.Length);
        return buf;
    }

    public static string DeserializeString(Stream s) {
        var len = DeserializeInt(s);
        var buf = new byte[len];
        var n = s.Read(buf, 0, buf.Length);
        Debug.Assert(n == buf.Length);
        return Encoding.UTF8.GetString(buf);
    }

    public static IList<T> DeserializeList<T>(Stream s) where T : IJuteDeserializable, new() {
        var len = DeserializeInt(s);
        var values = new List<T>();

        for (var i = 0; i < len; i++) {
            var value = new T();
            value.DeserializeFrom(s);
            values.Add(value);
        }

        return values;
    }

    public static IList<string> DeserializeStringList(Stream s) {
        var len = DeserializeInt(s);
        var values = new List<string>();

        for (var i = 0; i < len; i++) values.Add(DeserializeString(s));

        return values;
    }

    public static Dictionary<TKey, TValue> DeserializeDictionary<TKey, TValue>(Stream s)
        where TKey : IJuteDeserializable, new()
        where TValue : IJuteDeserializable, new() {
        var len = DeserializeInt(s);
        var dict = new Dictionary<TKey, TValue>();

        for (var i = 0; i < len; i++) {
            var key = new TKey();
            key.DeserializeFrom(s);
            var value = new TValue();
            value.DeserializeFrom(s);
            dict.Add(key, value);
        }

        return dict;
    }

    public static T Deserialize<T>(Stream stream) where T : IJuteDeserializable, new() {
        var value = new T();
        value.DeserializeFrom(stream);
        return value;
    }

    public static T Deserialize<T>(byte[] buffer) where T : IJuteDeserializable, new() {
        return Deserialize<T>(new MemoryStream(buffer));
    }
}