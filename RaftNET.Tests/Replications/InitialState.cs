﻿using RaftNET.Services;

namespace RaftNET.Tests.Replications;

public record InitialState {
    public ConfigMember Address = new();
    public ulong Term = 1;
    public ulong Vote;
    public IList<LogEntry> Log = new List<LogEntry>();
    public SnapshotDescriptor Snapshot;
    public SnapshotValue SnapshotValue;

    public RaftService.Options ServerConfig = new() {
        AppendRequestThreshold = 200
    };
}