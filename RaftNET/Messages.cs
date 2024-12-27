using System.Diagnostics;
using System.Text;
using Google.Protobuf;
using Google.Protobuf.Collections;

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

    public static Configuration ConfigFromIds(IEnumerable<ulong> current, IEnumerable<ulong> previous) {
        var cfg = new Configuration();

        foreach (var id in current) {
            cfg.Current.Add(new ConfigMember {
                ServerAddress = new ServerAddress {
                    ServerId = id
                },
                CanVote = true
            });
        }

        foreach (var id in previous) {
            cfg.Previous.Add(new ConfigMember {
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


    public static void EnterJoint(
        this Configuration configuration, RepeatedField<ConfigMember> members
    ) {
        var s = members.ToDictionary(member => member.ServerAddress.ServerId);
        EnterJoint(configuration, new HashSet<ConfigMember>(s.Values));
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

    public static bool CanVote(this Configuration configuration, ulong id) {
        return configuration.Current.Any(x => x.ServerAddress.ServerId == id && x.CanVote) ||
               configuration.Previous.Any(x => x.ServerAddress.ServerId == id && x.CanVote);
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

        foreach (var id in memberIds) {
            s.Add(CreateConfigMember(id));
        }

        return s;
    }

    public static void CheckConfiguration(RepeatedField<ConfigMember> cNew) {
        if (cNew.Count == 0) {
            throw new ArgumentException("Attempt to transition to an empty Raft configuration");
        }

        if (!cNew.Any(x => x.CanVote)) {
            throw new ArgumentException("The configuration must have at least one voter");
        }
    }

    public static int EntrySize(this LogEntry entry) {
        Debug.Assert(entry.Dummy != null || entry.Command != null || entry.Configuration != null);
        if (entry.Command != null) {
            return entry.Command.Buffer.Length;
        }

        if (entry.Configuration != null) {
            var size = 0;
            foreach (var member in entry.Configuration.Current) {
                size += member.CalculateSize();
            }
            return size;
        }

        return 0; // dummy
    }
}