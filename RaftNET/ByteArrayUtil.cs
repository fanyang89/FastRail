using System.Buffers.Binary;

namespace RaftNET;

public static class ByteArrayUtil {
    private static byte[] Concat(byte[] a, byte[] b) {
        var buf = new byte[a.Length + b.Length];
        a.CopyTo(new Memory<byte>(buf));
        b.CopyTo(new Memory<byte>(buf, a.Length, b.Length));
        return buf;
    }

    public static byte[] Concat(byte[] a, ulong b, bool bigEndian = true) {
        var buf = new byte[sizeof(ulong)];
        if (bigEndian) {
            BinaryPrimitives.WriteUInt64BigEndian(buf, b);
        } else {
            BinaryPrimitives.WriteUInt64LittleEndian(buf, b);
        }
        return Concat(a, buf);
    }

    public static bool StartsWith(this byte[] b, byte[] prefix) {
        if (prefix.Length > b.Length) {
            return false;
        }

        for (var i = 0; i < prefix.Length; ++i) {
            if (prefix[i] != b[i]) {
                return false;
            }
        }

        return true;
    }

    public static ulong GetIdx(byte[] b, byte[] prefix, bool bigEndian = true) {
        var s = new Memory<byte>(b, prefix.Length, sizeof(ulong)).Span;
        if (bigEndian) {
            return BinaryPrimitives.ReadUInt64BigEndian(s);
        }
        return BinaryPrimitives.ReadUInt64LittleEndian(s);
    }
}