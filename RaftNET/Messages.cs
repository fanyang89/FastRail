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
                },
                CanVote = true
            });
        }

        return cfg;
    }

    public static LogEntry CreateDummy(ulong idx = 0, ulong term = 0) {
        return new LogEntry {
            Idx = idx,
            Term = term,
            Dummy = new Void()
        };
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