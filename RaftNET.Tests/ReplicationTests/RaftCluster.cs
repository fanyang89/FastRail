using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RaftNET.Services;

namespace RaftNET.Tests.ReplicationTests;

public class RaftCluster {
    private List<RaftTestServer> _servers;
    private int _leader;
    private Dictionary<int, System.Timers.Timer> _tickers;
    private ISet<int> _inConfiguration;
    private readonly ILogger<RaftCluster> _logger;
    private IList<TimeSpan> _tickDelays;
    private TimeSpan _tickDelta;
    private int _nextValue;
    private bool _preVote;
    private Connected _connected;
    private int _applyEntries;
    private bool _verifyPersistedSnapshots;
    private PersistedSnapshots _persistedSnapshots;
    private CancellationTokenSource _cts = new();
    private Snapshots _snapshots;

    public RaftCluster(
        ReplicationTestCase test, ApplyFn apply,
        int applyEntries, int firstVal, int firstLeader, bool prevote, TimeSpan tickDelta,
        RpcConfig rpcConfig, ILogger<RaftCluster>? logger = null) {
        _logger = logger ?? new NullLogger<RaftCluster>();
    }

    public async Task StartAll() {
        foreach (var server in _servers) {
            await server.Start();
        }
        await InitRaftTickers();
        _logger.LogInformation("Electing first leader {}", _leader);
        _servers[_leader].Service.WaitUntilCandidate();
        await _servers[_leader].Service.WaitElectionDone();
    }

    private async Task InitRaftTickers() {
        foreach (var s in _inConfiguration) {
            var ticker = new System.Timers.Timer(_tickDelta);
            ticker.AutoReset = true;
            ticker.Elapsed += (_, _) => {
                _servers[s].Service.Tick(null);
            };
            ticker.Start();
            _tickers.Add(s, ticker);
        }
        await RestartTickers();
    }

    public void InitTickDelays(int n) {
        for (int s = 0; s < n; s++) {
            var delay = Random.Shared.Next(0, _tickDelta.Milliseconds);
            _tickDelays.Add(delay * _tickDelta / _tickDelta.Milliseconds);
        }
    }

    public RaftService GetServer(int id) {
        return _servers[id].Service;
    }

    private async Task RestartTickers() {
        if (_tickDelays.Count > 0) {
            await Task.WhenAll(_inConfiguration.Select(async s => {
                await Task.Delay(_tickDelays[s]);
                // TODO: make confirm we should use change()
            }));
        } else {
            foreach (var s in _inConfiguration) {}
        }
    }

    public async Task AddEntriesConcurrent(int n, int? server) {
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

    public async Task AddEntries(int n, int? server = null) {
        var end = _nextValue + n;
        while (_nextValue != end) {
            AddEntry(_nextValue, server);
            _nextValue++;
        }
        await Task.CompletedTask;
    }

    private void AddEntry(int value, int? server) {
        while (true) {
            try {
                var at = _servers[server ?? _leader].Service;
                at.AddEntry(value, WaitType.Committed);
                break;
            }
            catch (Exception e) {
                throw new NotImplementedException();
            }
        }
    }

    public async Task ElectNewLeader(int newLeader) {
        Debug.Assert(newLeader < _servers.Count, "wrong next leader: newLeader < _servers.Count");
        if (newLeader == _leader) {
            return;
        }

        if (_preVote) {
            // pre-vote
            var bothConnected = _connected.IsConnected(_leader, newLeader);
            if (bothConnected) {
                await WaitLog(newLeader);
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
            await RestartTickers();
            await WaitLogAll();
        } else {
            // not pre-vote
            do {
                if (_connected.IsConnected(_leader, newLeader)) {
                    await WaitLog(newLeader);
                }

                PauseTickers();
                var prevDisconnected = _connected.Clone();
                _connected.Disconnect(_leader);
                ElapseElections();
                await Task.Yield();
                _servers[newLeader].Service.WaitUntilCandidate();
                _connected.Connect(_leader);
                _connected.Disconnect(_leader, newLeader);
                await RestartTickers();
                await _servers[newLeader].Service.WaitElectionDone();

                _connected = prevDisconnected;
            } while (!_servers[newLeader].Service.IsLeader());
        }
    }

    private async Task WaitLogAll() {
        var leaderLogIdxTerm = _servers[_leader].Service.LogLastIdxTerm();
        for (var s = 0; s < _servers.Count; s++) {
            if (s != _leader && _connected.IsConnected(s, _leader) && _inConfiguration.Contains(s)) {
                await _servers[s].Service.WaitLogIdxTerm(leaderLogIdxTerm);
            }
        }
    }

    private void ElapseElections() {
        foreach (var s in _inConfiguration) {
            _servers[s].Service.ElapseElection();
        }
    }

    private void PauseTickers() {
        foreach (var (_, ticker) in _tickers) {
            ticker.Stop();
        }
    }

    public async Task Reset(Reset reset) {
        await ResetServer(reset.Id, reset.State);
    }

    private async Task ResetServer(int id, InitialState state) {
        _servers[id] = CreateService(id, state);
        await _servers[id].Service.StartAsync(_cts.Token);
        SetTickerCallback(id);
    }

    private RaftTestServer CreateService(int id, InitialState state) {
        throw new NotImplementedException();
    }

    private void SetTickerCallback(int id) {
        throw new NotImplementedException();
    }

    public async Task WaitLog(int follower) {
        if (_connected.IsConnected(_leader, follower) && _inConfiguration.Contains(_leader) &&
            _inConfiguration.Contains(follower)) {
            var leaderLogIdxTerm = _servers[_leader].Service.LogLastIdxTerm();
            await _servers[follower].Service.WaitLogIdxTerm(leaderLogIdxTerm);
        }
    }

    public async Task WaitLog(WaitLog followers) {
        var leaderLogIdxTerm = _servers[_leader].Service.LogLastIdxTerm();
        foreach (var s in followers.IntIds) {
            await _servers[s].Service.WaitLogIdxTerm(leaderLogIdxTerm);
        }
    }

    public async Task ChangeConfiguration(SetConfig sc) {
        Assert.That(sc, Is.Not.Empty, "Empty configuration change not supported");
    }

    public async Task Tick(Tick tick) {
        throw new NotImplementedException();
    }

    public async Task Read(ReadValue readValue) {
        throw new NotImplementedException();
    }

    public async Task CheckRpcConfig(CheckRpcConfig cc) {
        var addressSet = cc.Addresses;
        foreach (var node in cc.Nodes) {
            Assert.That(node.Id, Is.LessThan(_servers.Count));
            var peers = _servers[node.Id].Rpc.KnownPeers;
            Assert.That(peers, Is.EqualTo(addressSet));
        }
    }

    public async Task CheckRpcAdded(CheckRpcAdded checkRpcAdded) {
        throw new NotImplementedException();
    }

    public async Task CheckRpcRemoved(CheckRpcRemoved checkRpcRemoved) {
        throw new NotImplementedException();
    }

    public async Task RpcResetCounters(RpcResetCounters rpcResetCounters) {
        throw new NotImplementedException();
    }

    public async Task Partition(Partition partition) {
        throw new NotImplementedException();
    }

    public async Task Isolate(Isolate isolate) {
        _logger.LogInformation("Disconnecting id={}", isolate.Id);
        _connected.Disconnect(isolate.Id);
        if (isolate.Id == _leader) {
            _servers[_leader].Service.ElapseElection();
            await FreeElection();
        }
    }

    private async Task FreeElection() {
        throw new NotImplementedException();
    }

    public async Task Disconnect(Disconnect nodes) {
        _connected.Cut(nodes.First, nodes.Second);
    }

    public async Task Stop(Stop stop) {
        await StopServer(stop.Id);
    }

    private async Task StopServer(int id, string reason = "") {
        CancelTicker(id);
        await _servers[id].Service.StopAsync(_cts.Token);
        if (_snapshots.TryGetValue(id, out var snapshots)) {
            Assert.That(snapshots, Has.Count.LessThanOrEqualTo(2));
            _snapshots.Remove(id);
        }
        _persistedSnapshots.Remove(id);
    }

    private void CancelTicker(int id) {
        throw new NotImplementedException();
    }

    public void ConnectAll() {
        _connected.ConnectAll();
    }

    public void ReconfigureAll() {
        throw new NotImplementedException();
    }

    public async Task AddRemainingEntries() {
        await AddEntries(_applyEntries - _nextValue);
    }

    public async Task WaitAll() {
        foreach (var s in _inConfiguration) {
            await _servers[s].StateMachine.Done();
        }
    }

    public async Task StopAll() {
        foreach (var s in _inConfiguration) {
            await StopServer(s);
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

    public void Disconnect(int id, int? except) {
        _connected.Disconnect(id, except);
    }
}
