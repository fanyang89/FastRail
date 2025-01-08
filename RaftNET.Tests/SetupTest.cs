using System.Diagnostics;
using System.Net;
using NUnit.Framework.Diagnostics;

namespace RaftNET.Tests;

[SetUpFixture]
public class SetupTest {
    [OneTimeSetUp]
    public void Setup() {
        // setup progress trace
        if (!Trace.Listeners.OfType<ProgressTraceListener>().Any()) {
            Trace.Listeners.Add(new ProgressTraceListener());
        }

        // disable HTTP proxy
        HttpClient.DefaultProxy = new WebProxy();
    }
}