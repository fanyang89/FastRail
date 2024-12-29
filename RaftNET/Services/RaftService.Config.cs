using Microsoft.Extensions.Logging;

namespace RaftNET.Services;

public partial class RaftService {
    public class Config {
        public string DataDir { get; set; }
        public ulong MyId { get; set; }
        public IStateMachine StateMachine { get; set; }
        public ILoggerFactory LoggerFactory { get; set; }
        public bool EnablePreVote { get; set; } = true;
        public Action<Event> OnEvent { get; set; }
        public AddressBook AddressBook { get; set; }
        public int MaxLogSize { get; set; } = 4 * 1024 * 1024;
        public int AppendRequestThreshold { get; set; }
    }
}