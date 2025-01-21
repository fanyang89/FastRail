using System.Diagnostics;
using Google.Protobuf;
using RaftNET.Services;
using Serilog;

namespace RaftNET.Tests.ReplicationTests;

public class RaftCluster {
    private readonly ApplyFn _apply;
    private readonly ulong _applyEntries;
    private readonly SortedSet<ulong> _inConfiguration = [];
    private readonly PersistedSnapshots _persistedSnapshots;
    private readonly bool _preVote;
    private readonly RpcConfig _rpcConfig;
    private readonly RpcNet _rpcNet = new();
    private readonly Dictionary<ulong, RaftTestServer> _servers = new();
    private readonly Snapshots _snapshots;
    private readonly CancellationTokenSource _startCancellationTokenSource = new();
    private readonly List<TimeSpan> _tickDelays = [];
    private readonly TimeSpan _tickDelta;
    private readonly Dictionary<ulong, System.Timers.Timer> _tickers = new();
    private readonly bool _verifyPersistedSnapshots;
    private Connected _connected;
    private ulong _leader;
    private ulong _nextValue;

    public RaftCluster(
        ReplicationTestCase test, ApplyFn apply,
        ulong applyEntries, ulong firstVal, ulong firstLeader, bool preVote, TimeSpan tickDelta,
        RpcConfig rpcConfig) {
        _connected = new Connected(test.Nodes);
        _snapshots = new Snapshots();
        _persistedSnapshots = new PersistedSnapshots();
        _applyEntries = applyEntries;
        _nextValue = firstVal;
        _rpcConfig = rpcConfig;
        _preVote = preVote;
        _apply = apply;
        _leader = firstLeader;
        _tickDelta = tickDelta;
        _verifyPersistedSnapshots = test.VerifyPersistedSnapshots;

        var states = GetStates(test, preVote);
        for (ulong s = 1; s <= (ulong)states.Count; s++) {
            _inConfiguration.Add(s);
        }

        var config = new Configuration();
        for (ulong i = 1; i <= (ulong)states.Count; i++) {
            states[i].Address = new ConfigMember {
                ServerAddress = new ServerAddress {
                    ServerId = i
                },
                CanVote = true
            };
            config.Current.Add(states[i].Address);
        }

        if (_rpcConfig.NetworkDelay > TimeSpan.Zero) {
            InitTickDelays(test.Nodes);
        }

        for (ulong i = 1; i <= (ulong)states.Count; i++) {
            var s = states[i].Address;
            states[i].Snapshot.Config = config;
            var serverId = s.ServerAddress.ServerId;
            if (!_snapshots.TryGetValue(serverId, out var snapshots)) {
                snapshots = new Dictionary<ulong, SnapshotValue>();
                _snapshots.Add(serverId, snapshots);
            }
            var snapshotId = states[i].Snapshot.Id;
            snapshots[snapshotId] = states[i].SnapshotValue;
            _servers.Add(i, CreateService(i, states[i]));
        }
    }

    public async Task AddEntriesAsync(ulong n, ulong? server = null) {
        var end = _nextValue + n;
        while (_nextValue != end) {
            AddEntry(_nextValue, server);
            _nextValue++;
        }
        await Task.CompletedTask;
    }

    public async Task AddEntriesConcurrentAsync(ulong n, ulong? server) {
        var start = _nextValue;
        _nextValue += n;
        var tasks = new List<Task>();
        for (var i = start; i < _nextValue; i++) {
            var v = i;
            tasks.Add(Task.Run(() => {
                AddEntry(v, server);
            }));
        }
        await Task.WhenAll(tasks);
    }

    public async Task AddRemainingEntriesAsync() {
        await AddEntriesAsync(_applyEntries - _nextValue);
    }

    public async Task ChangeConfigurationAsync(SetConfig sc) {
        Assert.That(sc, Is.Not.Empty, "Empty configuration change not supported");
    }

    public async Task CheckRpcAddedAsync(CheckRpcAdded checkRpcAdded) {
        throw new NotImplementedException();
    }

    public async Task CheckRpcConfigAsync(CheckRpcConfig cc) {
        var addressSet = cc.Addresses;
        foreach (var node in cc.Nodes) {
            Assert.That(node.Id, Is.LessThan(_servers.Count));
            var peers = _servers[node.Id].Rpc.KnownPeers;
            Assert.That(peers, Is.EqualTo(addressSet));
        }
    }

    public async Task CheckRpcRemovedAsync(CheckRpcRemoved checkRpcRemoved) {
        throw new NotImplementedException();
    }

    public void ConnectAll() {
        _connected.ConnectAll();
    }

    public void Disconnect(ulong id, ulong? except) {
        _connected.Disconnect(id, except);
    }

    public async Task DisconnectAsync(Disconnect nodes) {
        _connected.Cut(nodes.First, nodes.Second);
    }

    public async Task ElectNewLeaderAsync(ulong newLeader) {
        Debug.Assert(newLeader < (ulong)_servers.Count, "wrong next leader: newLeader < _servers.Count");
        if (newLeader == _leader) {
            return;
        }

        if (_preVote) {
            // pre-vote
            var bothConnected = _connected.IsConnected(_leader, newLeader);
            if (bothConnected) {
                await WaitLogAsync(newLeader);
            }

            PauseTickers();
            var prevDisconnected = _connected.Clone();
            _connected.Disconnect(_leader);
            ElapseElections();

            do {
                await Task.Yield();
                _servers[newLeader].Service.WaitUntilCandidate();

                if (bothConnected) {
                    _connected.Connect(_leader);
                    _connected.Disconnect(_leader, newLeader);
                }
                await _servers[newLeader].Service.WaitElectionDone();

                if (bothConnected) {
                    _connected.Disconnect(_leader);
                }
            } while (!_servers[newLeader].Service.IsLeader());

            _connected = prevDisconnected;
            await RestartTickersAsync();
            await WaitLogAllAsync();
        } else {
            // not pre-vote
            do {
                if (_connected.IsConnected(_leader, newLeader)) {
                    await WaitLogAsync(newLeader);
                }

                PauseTickers();
                var prevDisconnected = _connected.Clone();
                _connected.Disconnect(_leader);
                ElapseElections();
                await Task.Yield();
                _servers[newLeader].Service.WaitUntilCandidate();
                _connected.Connect(_leader);
                _connected.Disconnect(_leader, newLeader);
                await RestartTickersAsync();
                await _servers[newLeader].Service.WaitElectionDone();

                _connected = prevDisconnected;
            } while (!_servers[newLeader].Service.IsLeader());
        }
    }

    public RaftService GetServer(ulong id) {
        return _servers[id].Service;
    }

    public void InitTickDelays(ulong n) {
        for (ulong s = 0; s < n; s++) {
            var delay = Random.Shared.Next(0, _tickDelta.Milliseconds);
            _tickDelays.Add(delay * _tickDelta / _tickDelta.Milliseconds);
        }
    }

    public async Task IsolateAsync(Isolate isolate) {
        Log.Information("Disconnecting id={id}", isolate.Id);
        _connected.Disconnect(isolate.Id);
        if (isolate.Id == _leader) {
            _servers[_leader].Service.ElapseElection();
            await FreeElectionAsync();
        }
    }

    public async Task PartitionAsync(Partition partition) {
        throw new NotImplementedException();
    }

    public async Task ReadAsync(ReadValue r) {
        await _servers[r.NodeIdx].Service.ReadBarrier(null);
        var value = _servers[r.NodeIdx].StateMachine.Hasher.FinalizeUInt64();
        var expected = HasherInt.HashRange(r.ExpectedIdx).FinalizeUInt64();
        Assert.That(value, Is.EqualTo(expected), $"Read on server {r.NodeIdx} saw the wrong value {value} != {expected}");
    }

    public async Task ReconfigureAllAsync() {
        if (_inConfiguration.Count < _servers.Count) {
            var sc = new SetConfig();
            foreach (var (id, s) in _servers) {
                sc.Add(new SetConfigEntry(id, true));
            }
            await ChangeConfigurationAsync(sc);
        }
    }

    public async Task ResetAsync(Reset reset) {
        await ResetServerAsync(reset.Id, reset.State);
    }

    public async Task RpcResetCountersAsync(RpcResetCounters rpcResetCounters) {
        throw new NotImplementedException();
    }

    public async Task StartAllAsync() {
        await Task.WhenAll(_servers.Values.Select(s => s.Service.StartAsync(_startCancellationTokenSource.Token)));
        await InitRaftTickersAsync();
        Log.Information("Electing first leader {leader}", _leader);
        _servers[_leader].Service.WaitUntilCandidate();
        await _servers[_leader].Service.WaitElectionDone();
    }

    public async Task StopAllAsync(string reason = "") {
        foreach (var s in _inConfiguration) {
            await StopServerAsync(s, reason);
        }
    }

    public async Task StopAsync(Stop stop) {
        await StopServerAsync(stop.Id);
    }

    public async Task TickAsync(Tick tick) {
        for (ulong i = 0; i < tick.Ticks; i++) {
            foreach (var (_, s) in _servers) {
                s.Service.Tick();
            }
        }
    }

    public void Verify() {
        {
            var expected = HasherInt.HashRange(_applyEntries).FinalizeUInt64();
            foreach (var i in _inConfiguration) {
                var digest = _servers[i].StateMachine.Hasher.FinalizeUInt64();
                Assert.That(digest, Is.EqualTo(expected), "Digest doesn't match");
            }
        }

        if (_verifyPersistedSnapshots) {
            foreach (var (snapshot, value) in _persistedSnapshots) {
                var (snp, val) = value;
                var digest = val.Hasher.FinalizeUInt64();
                var expected = HasherInt.HashRange(val.Idx).FinalizeUInt64();
                Assert.That(digest, Is.EqualTo(expected), $"Persisted snapshot mismatch, id={snp.Id}");
            }
        }
    }

    public async Task WaitAllAsync() {
        foreach (var s in _inConfiguration) {
            await _servers[s].StateMachine.DoneAsync();
        }
    }

    public async Task WaitLogAsync(ulong follower) {
        if (_connected.IsConnected(_leader, follower) && _inConfiguration.Contains(_leader) &&
            _inConfiguration.Contains(follower)) {
            var leaderLogIdxTerm = _servers[_leader].Service.LogLastIdxTerm();
            await _servers[follower].Service.WaitLogIdxTerm(leaderLogIdxTerm);
        }
    }

    public async Task WaitLogAsync(WaitLog followers) {
        var leaderLogIdxTerm = _servers[_leader].Service.LogLastIdxTerm();
        foreach (var s in followers.IntIds) {
            await _servers[s].Service.WaitLogIdxTerm(leaderLogIdxTerm);
        }
    }

    private void AddEntry(ulong value, ulong? server) {
        while (true) {
            try {
                var at = _servers[server ?? _leader].Service;
                at.AddEntry(value, WaitType.Committed);
                break;
            }
            catch (Exception e) {
                // TODO: handle AddEntry() exception
                throw new NotImplementedException();
            }
        }
    }

    private void CancelTicker(ulong id) {
        _tickers[id].Stop();
    }

    private IList<LogEntry> CreateLog(List<LogEntrySlim> list, ulong startIdx) {
        var log = new List<LogEntry>();
        var i = startIdx;
        foreach (var e in list) {
            if (e.Data.IsT0) {
                var buffer = BitConverter.GetBytes(e.Data.AsT0);
                log.Add(new LogEntry {
                    Term = e.Term,
                    Idx = i++,
                    Command = new Command {
                        Buffer = ByteString.CopyFrom(buffer)
                    }
                });
            } else if (e.Data.IsT1) {
                log.Add(new LogEntry {
                    Term = e.Term,
                    Idx = i++,
                    Configuration = e.Data.AsT1
                });
            } else {
                throw new UnreachableException();
            }
        }
        return log;
    }

    private RaftTestServer CreateService(ulong id, InitialState state) {
        var sm = new TestStateMachine(id, _apply, _applyEntries, _snapshots);
        var rpc = new MockRpc(id, _connected, _snapshots, _rpcNet, _rpcConfig);
        var persistence = new MockPersistence(id, state, _snapshots, _persistedSnapshots);
        var fd = new MockFailureDetector(id, _connected);
        var addressBook = new AddressBook();
        var raft = new RaftService(id, rpc, sm, persistence, fd, addressBook, state.ServerConfig);
        return new RaftTestServer(raft, sm, rpc);
    }

    private void ElapseElections() {
        foreach (var s in _inConfiguration) {
            _servers[s].Service.ElapseElection();
        }
    }

    private async Task FreeElectionAsync() {
        Log.Information("Running free election");
        var loops = 0;
        for (;; loops++) {
            await Task.Delay(_tickDelta);
            foreach (var s in _inConfiguration) {
                if (!_servers[s].Service.IsLeader()) continue;
                Log.Information("New leader, id={id} loops={loops}", s, loops);
                _leader = s;
                return;
            }
        }
    }

    private Dictionary<ulong, InitialState> GetStates(ReplicationTestCase test, bool preVote) {
        var states = new Dictionary<ulong, InitialState>();
        for (ulong i = 1; i <= test.Nodes; i++) {
            states.Add(i, new InitialState());
        }

        var leader = test.InitialLeader;
        states[leader].Term = test.InitialTerm;

        for (ulong i = 1; i <= (ulong)states.Count; i++) {
            ulong startIdx = 1;
            if (i < (ulong)test.InitialSnapshots.Count) {
                states[i].Snapshot = test.InitialSnapshots[i];
                states[i].SnapshotValue.Hasher = HasherInt.HashRange(test.InitialSnapshots[i].Idx);
                states[i].SnapshotValue.Idx = test.InitialSnapshots[i].Idx;
                startIdx = states[i].Snapshot.Idx + 1;
            }
            if (i < (ulong)test.InitialStates.Count) {
                var state = test.InitialStates[i];
                states[i].Log = CreateLog(state, startIdx);
            } else {
                states[i].Log = [];
            }
            if (i < (ulong)test.Config.Count) {
                states[i].ServerConfig = test.Config[i];
            } else {
                states[i].ServerConfig = new RaftServiceOptions {
                    EnablePreVote = preVote
                };
            }
        }
        return states;
    }

    private async Task InitRaftTickersAsync() {
        foreach (var s in _inConfiguration) {
            var ticker = new System.Timers.Timer(_tickDelta);
            ticker.AutoReset = true;
            ticker.Elapsed += (_, _) => {
                _servers[s].Service.Tick(null);
            };
            ticker.Start();
            _tickers.Add(s, ticker);
        }
        await RestartTickersAsync();
    }

    private void PauseTickers() {
        foreach (var (_, ticker) in _tickers) {
            ticker.Stop();
        }
    }

    private async Task ResetServerAsync(ulong id, InitialState state) {
        _servers[id] = CreateService(id, state);
        await _servers[id].Service.StartAsync(_startCancellationTokenSource.Token);
        SetTickerCallback(id);
    }

    private async Task RestartTickersAsync() {
        if (_tickDelays.Count > 0) {
            await Task.WhenAll(_inConfiguration.Select(async s => {
                await Task.Delay(_tickDelays[(int)s]);
                // TODO: make confirm we should use change()
            }));
        } else {
            foreach (var s in _inConfiguration) {}
        }
    }

    private void SetTickerCallback(ulong id) {
        throw new NotImplementedException();
    }

    private async Task StopServerAsync(ulong id, string reason = "") {
        Log.Information("Stopping server {id}, reason: {reason}", id, reason);

        CancelTicker(id);
        await _startCancellationTokenSource.CancelAsync();
        var cancelStop = new CancellationTokenSource();
        await _servers[id].Service.StopAsync(cancelStop.Token);
        if (_snapshots.TryGetValue(id, out var snapshots)) {
            Assert.That(snapshots, Has.Count.LessThanOrEqualTo(2));
            _snapshots.Remove(id);
        }
        _persistedSnapshots.Remove(id);
    }

    private async Task WaitLogAllAsync() {
        var leaderLogIdxTerm = _servers[_leader].Service.LogLastIdxTerm();
        for (ulong s = 0; s < (ulong)_servers.Count; s++) {
            if (s != _leader && _connected.IsConnected(s, _leader) && _inConfiguration.Contains(s)) {
                await _servers[s].Service.WaitLogIdxTerm(leaderLogIdxTerm);
            }
        }
    }
}
