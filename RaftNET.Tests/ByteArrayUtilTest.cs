namespace RaftNET.Tests;

public class ByteArrayUtilTest {
    [Test]
    public void ConcatByteBuffers() {
        var keyLogEntryPrefix = "/log/"u8.ToArray();
        var buf = ByteArrayUtil.Concat(keyLogEntryPrefix, 0x12345678);
        Assert.That(buf, Is.EqualTo("/log/\x00\x00\x00\x00\x12\x34\x56\x78"u8.ToArray()));

        buf = ByteArrayUtil.Concat(keyLogEntryPrefix, 0x12345678, false);
        Assert.That(buf, Is.EqualTo("/log/\x78\x56\x34\x12\x00\x00\x00\x00"u8.ToArray()));
    }

    [Test]
    public void GetIdx() {
        Assert.That(
            ByteArrayUtil.GetIdx(
                "/log/\x00\x00\x00\x00\x12\x34\x56\x78"u8.ToArray(),
                "/log/"u8.ToArray()
            ),
            Is.EqualTo(0x12345678)
        );

        Assert.That(
            ByteArrayUtil.GetIdx(
                "/log/\x78\x56\x34\x12\x00\x00\x00\x00"u8.ToArray(),
                "/log/"u8.ToArray(),
                false
            ),
            Is.EqualTo(0x12345678)
        );
    }
}
