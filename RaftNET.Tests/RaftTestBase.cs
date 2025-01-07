﻿using Microsoft.Extensions.Logging;

namespace RaftNET.Tests;

[TestFixture]
public class RaftTestBase {
    [SetUp]
    public void Setup() {
        Console.SetOut(TestContext.Progress);
        LoggerFactory = RaftNET.LoggerFactory.Instance;
    }

    protected ILoggerFactory LoggerFactory;
}