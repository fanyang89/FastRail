namespace RaftNET.Services;

public class AddressBook {
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
}