using System.Text;

namespace FastRail.Server;

public static class Strings {
    public static byte[] ToBytes(this string value) {
        return Encoding.UTF8.GetBytes(value);
    }

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
}