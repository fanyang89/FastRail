using org.apache.zookeeper;
using org.apache.zookeeper.data;

namespace FastRail.Tests;

// ReSharper disable once InconsistentNaming
public static class ACLs {
    public static readonly ACL WorldAnyone = new((int)ZooDefs.Perms.ALL, ZooDefs.Ids.ANYONE_ID_UNSAFE);
}