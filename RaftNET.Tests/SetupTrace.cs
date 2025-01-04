using System.Diagnostics;
using NUnit.Framework.Diagnostics;

namespace RaftNET.Tests;

[SetUpFixture]
public class SetupTrace {
    [OneTimeSetUp]
    public void Setup() {
        if (!Trace.Listeners.OfType<ProgressTraceListener>().Any()) {
            Trace.Listeners.Add(new ProgressTraceListener());
        }
    }
}