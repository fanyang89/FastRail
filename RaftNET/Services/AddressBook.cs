using System.Net;

namespace RaftNET.Services;

public class AddressBook : IAddressBook {
    private readonly Dictionary<ulong, string> _addresses = new();

    public AddressBook() {}

    public AddressBook(List<string> initialMembers) {
        foreach (var parts in initialMembers.Select(memberString => memberString.Split('='))) {
            if (parts.Length != 2) {
                throw new ArgumentException("Invalid member format", nameof(initialMembers));
            }

            var id = ulong.Parse(parts[0]);
            var endpoint = parts[1];
            if (!endpoint.StartsWith("http://")) {
                endpoint = "http://" + endpoint;
            }
            _addresses.Add(id, endpoint);
        }
    }

    public void Add(ulong id, string address) {
        lock (_addresses) {
            _addresses.Add(id, address);
        }
    }

    public string? Find(ulong id) {
        lock (_addresses) {
            return _addresses.GetValueOrDefault(id);
        }
    }

    public void Remove(ulong id) {
        lock (_addresses) {
            _addresses.Remove(id);
        }
    }

    public Dictionary<ulong, bool> GetMembers() {
        lock (_addresses) {
            return _addresses.Select(x => (x.Key, true)).ToDictionary();
        }
    }

    public void Add(ulong id, string address, int port) {
        Add(id, $"http://{address}:{port}");
    }

    public void Add(ulong id, IPAddress address, int port) {
        Add(id, $"http://{address}:{port}");
    }

    public void Add(ulong id, int port) {
        Add(id, $"http://127.0.0.1:{port}");
    }
}
