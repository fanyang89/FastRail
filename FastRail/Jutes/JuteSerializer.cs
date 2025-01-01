using System.Buffers.Binary;

namespace FastRail.Jutes;

public static class JuteSerializer {
    public static void SerializeTo(Stream s, bool? value) {
        if (value == null) {
            return;
        }

        s.WriteByte(value.Value ? (byte)1 : (byte)0);
    }

    public static void SerializeTo(Stream s, byte? value) {
        if (value == null) {
            return;
        }

        s.WriteByte(value.Value);
    }

    public static void SerializeTo(Stream s, int? value) {
        if (value == null) {
            return;
        }

        var buf = new byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(buf, value.Value);
        s.Write(buf, 0, buf.Length);
    }

    public static void SerializeTo(Stream s, long? value) {
        if (value == null) {
            return;
        }

        var buf = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(buf, value.Value);
        s.Write(buf, 0, buf.Length);
    }

    public static void SerializeTo(Stream s, float? value) {
        if (value == null) {
            return;
        }

        var buf = new byte[sizeof(float)];
        BinaryPrimitives.WriteSingleBigEndian(buf, value.Value);
        s.Write(buf, 0, buf.Length);
    }

    public static void SerializeTo(Stream s, double? value) {
        if (value == null) {
            return;
        }

        var buf = new byte[sizeof(double)];
        BinaryPrimitives.WriteDoubleBigEndian(buf, value.Value);
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

    public static byte[] Serialize<T>(T? value) where T : IJuteSerializable {
        if (value == null) {
            return [];
        }

        using var ms = new MemoryStream();
        SerializeTo(ms, value);
        return ms.ToArray();
    }
}