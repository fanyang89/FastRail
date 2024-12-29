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
}