using System.Numerics;

namespace RaftNET;

internal class MatchVector<T> where T : INumber<T> {
    private int _count;
    private readonly List<T> _match;
    private readonly T _prevCommitIdx;

    public MatchVector(T prevCommitIdx, int reserveSize) {
        _prevCommitIdx = prevCommitIdx;
        _match = new List<T>(reserveSize);
    }

    public void Add(T matchIdx) {
        if (matchIdx > _prevCommitIdx) {
            ++_count;
        }

        _match.Add(matchIdx);
    }

    public bool Committed() {
        return _count >= _match.Count / 2 + 1;
    }

    public T CommitIdx() {
        // The index of the pivot node is selected so that all nodes
        // with a larger match index plus the pivot form a majority,
        // for example:
        // cluster size  pivot node     majority
        // 1             0              1
        // 2             0              2
        // 3             1              2
        // 4             1              3
        // 5             2              3
        var pivot = (_match.Count - 1) / 2;
        _match.Sort();
        return _match[pivot];
    }
}