namespace RaftNET.Services;

public class AddressBook {
    private readonly Dictionary<ulong, string> _addresses = new();

    public void Add(ulong id, string address) {
        _addresses.Add(id, address);
    }

    public string? Find(ulong id) {
        return _addresses.GetValueOrDefault(id);
    }
}