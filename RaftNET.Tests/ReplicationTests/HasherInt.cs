﻿using System.Diagnostics;
using System.IO.Hashing;

namespace RaftNET.Tests.ReplicationTests;

public class HasherInt {
    private readonly XxHash64? _hasher;
    private int? _hasherInt = 0;

    public HasherInt(bool commutative = false) {
        if (commutative) {
            _hasherInt = 0;
        } else {
            _hasher = new XxHash64();
        }
    }

    public ulong FinalizeUInt64() {
        if (_hasherInt != null) {
            return (ulong)_hasherInt.Value;
        }
        if (_hasher != null) {
            var hash = _hasher.GetHashAndReset();
            return BitConverter.ToUInt64(hash);
        }
        throw new UnreachableException();
    }

    public static HasherInt HashRange(ulong max, bool commutative = false) {
        var h = new HasherInt(commutative);
        for (var i = 0; i < (int)max; ++i) {
            h.Update(i);
        }
        return h;
    }

    public void Update(int val) {
        if (_hasherInt != null) {
            _hasherInt += val;
        }
        _hasher?.Append(BitConverter.GetBytes(val));
    }
}
