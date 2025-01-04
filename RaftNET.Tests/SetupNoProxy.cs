using System.Net;

namespace RaftNET.Tests;

[SetUpFixture]
public class SetupNoProxy {
    [OneTimeSetUp]
    public void Setup() {
        HttpClient.DefaultProxy = new WebProxy();
    }
}