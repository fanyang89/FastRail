namespace RaftNET.Services;

public class AddressBook : IAddressBook {
    private readonly Dictionary<ulong, string> _addresses = new();

    public AddressBook() {}

    public AddressBook(List<string> initialMembers) {
        foreach (var memberString in initialMembers) {
            // format: <id>=<address>
            var parts = memberString.Split('=');
            if (parts.Length != 2) {
                throw new ArgumentException(nameof(initialMembers));
            }
            _addresses.Add(ulong.Parse(parts[0]), parts[1]);
        }
    }

    public Dictionary<ulong, bool> GetMembers() {
        lock (_addresses) {
            return _addresses.Select(x => (x.Key, true)).ToDictionary();
        }
    }

    public void Add(ulong id, string address) {
        lock (_addresses) {
            _addresses.Add(id, address);
        }
    }

    public void Add(ulong id, int port) {
        Add(id, $"http://127.0.0.1:{port}");
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
}