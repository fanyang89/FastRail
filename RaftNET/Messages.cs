using System.Diagnostics;
using System.Text;
using Google.Protobuf;

namespace RaftNET;

public static class Messages {
    public static Configuration ConfigFromIds(params ulong[] ids) {
        var cfg = new Configuration();

        foreach (var id in ids)
            cfg.Current.Add(new ConfigMember {
                ServerAddress = new ServerAddress {
                    ServerId = id
                },
                CanVote = true
            });

        return cfg;
    }

    public static Configuration ConfigFromIds(IEnumerable<ulong> current, IEnumerable<ulong> previous) {
        var cfg = new Configuration();

        foreach (var id in current)
            cfg.Current.Add(new ConfigMember {
                ServerAddress = new ServerAddress {
                    ServerId = id
                },
                CanVote = true
            });

        foreach (var id in previous)
            cfg.Previous.Add(new ConfigMember {
                ServerAddress = new ServerAddress {
                    ServerId = id
                },
                CanVote = true
            });

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

    public static void EnterJoint(this Configuration configuration, ISet<ConfigMember> cNew) {
        if (cNew.Count == 0) {
            throw new ArgumentException(nameof(cNew));
        }

        configuration.Previous.Clear();
        configuration.Previous.AddRange(configuration.Current);
        configuration.Current.Clear();
        configuration.Current.AddRange(cNew);
    }

    public static bool IsJoint(this Configuration configuration) {
        return configuration.Previous.Count > 0;
    }


    public static void LeaveJoint(this Configuration configuration) {
        Debug.Assert(configuration.Previous.Count > 0);
        configuration.Previous.Clear();
    }

    public static ConfigMember CreateConfigMember(ulong id, bool canVote = true) {
        return new ConfigMember {
            ServerAddress = new ServerAddress {
                ServerId = id
            },
            CanVote = canVote
        };
    }

    public static ISet<ConfigMember> CreateConfigMembers(params ulong[] memberIds) {
        var s = new HashSet<ConfigMember>();

        foreach (var id in memberIds) s.Add(CreateConfigMember(id));

        return s;
    }
}