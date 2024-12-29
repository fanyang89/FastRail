namespace RaftNET.Services;

public interface IAddressBook {
    void Add(ulong id, string address);
    string? Find(ulong id);
    void Remove(ulong id);
}

public class AddressBook : IAddressBook {
    private readonly Dictionary<ulong, string> _addresses = new();

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