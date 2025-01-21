using System.Text;

namespace FastRail.Server;

public static class StringExtension {
    public static bool StartsWith(this byte[] buffer, string prefix) {
        if (prefix.Length > buffer.Length) {
            return false;
        }
        return !prefix.Where((c, i) => c != buffer[i]).Any();
    }

    public static bool StartsWith(this byte[] buffer, byte[] prefix) {
        if (prefix.Length > buffer.Length) {
            return false;
        }
        return !prefix.Where((c, i) => c != buffer[i]).Any();
    }

    public static bool StartsWith(this ArraySegment<byte> buffer, string prefix) {
        if (prefix.Length > buffer.Count) {
            return false;
        }
        return !prefix.Where((c, i) => c != buffer[i]).Any();
    }

    public static byte[] ToBytes(this string value) {
        return Encoding.UTF8.GetBytes(value);
    }

    public static string TrimPrefix(this string str, string prefix) {
        return str.StartsWith(prefix) ? str[prefix.Length..] : str;
    }

    public static byte[] TrimPrefix(this byte[] buffer, byte[] prefix) {
        return buffer.StartsWith(prefix) ? buffer[prefix.Length..] : buffer;
    }
}
