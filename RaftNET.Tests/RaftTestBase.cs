﻿using Serilog;

namespace RaftNET.Tests;

[TestFixture]
public class RaftTestBase {
    [SetUp]
    public void Setup() {
        Console.SetOut(TestContext.Progress);
        Log.Logger = new LoggerConfiguration().WriteTo.Console().CreateLogger();
    }
}
