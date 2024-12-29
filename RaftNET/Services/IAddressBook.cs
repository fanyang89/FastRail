namespace RaftNET.Services;

public interface IAddressBook {
    void Add(ulong id, string address);
    string? Find(ulong id);
    void Remove(ulong id);
}