﻿using System.Diagnostics;
using System.IO.Hashing;

namespace RaftNET.Tests.Replications;

public class HasherInt {
    private XxHash64? _hasher;
    private ulong? _hasher_int = 0;

    public HasherInt(bool commutative) {
        if (commutative) {
            _hasher_int = 0;
        } else {
            _hasher = new XxHash64();
        }
    }

    public void Update(int val) {
        if (_hasher_int != null) {
            _hasher_int += (ulong)val;
        }
        _hasher?.Append(BitConverter.GetBytes(val));
    }

    public ulong FinalizeUInt64() {
        if (_hasher_int != null) {
            return _hasher_int.Value;
        }
        if (_hasher != null) {
            var hash = _hasher.GetHashAndReset();
            return BitConverter.ToUInt64(hash);
        }
        throw new UnreachableException();
    }

    public static HasherInt HashRange(int max, bool commutative = false) {
        var h = new HasherInt(commutative);
        for (var i = 0; i < max; ++i) {
            h.Update(i);
        }
        return h;
    }
}
