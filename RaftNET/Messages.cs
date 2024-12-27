using System.Text;
using Google.Protobuf;

namespace RaftNET;

public static class Messages {
    public static Configuration ConfigFromIds(params ulong[] ids) {
        var cfg = new Configuration();

        foreach (var id in ids) {
            cfg.Current.Add(new ConfigMember {
                ServerAddress = new ServerAddress {
                    ServerId = id
                }
            });
        }

        return cfg;
    }

    public static LogEntry CreateDummy() {
        return new LogEntry { Dummy = new Void() };
    }

    public static LogEntry CreateCommand(string cmd) {
        return new LogEntry {
            Command = new Command {
                Buffer = ByteString.CopyFrom(cmd, Encoding.UTF8)
            }
        };
    }

    public static LogEntry CreateConfiguration(Configuration cfg) {
        return new LogEntry {
            Configuration = cfg
        };
    }
}