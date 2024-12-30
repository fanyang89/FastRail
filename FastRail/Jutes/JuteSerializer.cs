using System.Buffers.Binary;
using RocksDbSharp;

namespace FastRail.Jutes;

public static class JuteSerializer {
    public static void SerializeTo(Stream s, bool value) {
        s.WriteByte(value ? (byte)1 : (byte)0);
    }

    public static void SerializeTo(Stream s, byte value) {
        s.WriteByte(value);
    }

    public static void SerializeTo(Stream s, int value) {
        var buf = new byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(buf, value);
        s.Write(buf, 0, buf.Length);
    }

    public static void SerializeTo(Stream s, long value) {
        var buf = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(buf, value);
        s.Write(buf, 0, buf.Length);
    }

    public static void SerializeTo(Stream s, float value) {
        var buf = new byte[sizeof(float)];
        BinaryPrimitives.WriteSingleBigEndian(buf, value);
        s.Write(buf, 0, buf.Length);
    }

    public static void SerializeTo(Stream s, double value) {
        var buf = new byte[sizeof(double)];
        BinaryPrimitives.WriteDoubleBigEndian(buf, value);
        s.Write(buf, 0, buf.Length);
    }

    public static void SerializeTo(Stream s, string? value) {
        if (value == null) {
            return;
        }
        SerializeTo(s, value.Length);
        foreach (var c in value) {
            s.WriteByte((byte)c);
        }
    }

    public static void SerializeTo(Stream s, byte[]? value) {
        if (value == null) {
            return;
        }
        SerializeTo(s, value.Length);
        s.Write(value, 0, value.Length);
    }

    public static void SerializeTo(Stream s, IList<string>? values) {
        if (values == null) {
            return;
        }
        SerializeTo(s, values.Count);
        foreach (var value in values) {
            SerializeTo(s, value);
        }
    }

    public static void SerializeTo<T>(Stream s, IList<T>? values) where T : IJuteSerializable {
        if (values == null) {
            return;
        }
        SerializeTo(s, values.Count);
        foreach (var value in values) {
            value.SerializeTo(s);
        }
    }

    public static void SerializeTo<TKey, TValue>(Stream s, IDictionary<TKey, TValue>? values)
        where TKey : IJuteSerializable
        where TValue : IJuteSerializable {
        if (values == null) {
            return;
        }
        SerializeTo(s, values.Count);
        foreach (var (key, value) in values) {
            key.SerializeTo(s);
            value.SerializeTo(s);
        }
    }

    public static void SerializeTo<T>(Stream s, T? value) where T : IJuteSerializable {
        if (value == null) {
            return;
        }
        value.SerializeTo(s);
    }
}