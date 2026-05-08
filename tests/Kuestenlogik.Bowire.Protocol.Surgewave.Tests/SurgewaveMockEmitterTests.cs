// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using Kuestenlogik.Bowire.Mocking;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Bowire.Protocol.Surgewave.Tests;

public sealed class SurgewaveMockEmitterTests
{
    [Fact]
    public async Task CanEmit_TrueWhenRecordingHasSurgewaveStep()
    {
        await using var emitter = new SurgewaveMockEmitter();
        var rec = new BowireRecording
        {
            Steps =
            {
                new BowireRecordingStep { Protocol = "rest" },
                new BowireRecordingStep { Protocol = "surgewave" }
            }
        };
        Assert.True(emitter.CanEmit(rec));
    }

    [Fact]
    public async Task CanEmit_FalseWhenRecordingHasNoSurgewaveStep()
    {
        await using var emitter = new SurgewaveMockEmitter();
        var rec = new BowireRecording
        {
            Steps = { new BowireRecordingStep { Protocol = "kafka" } }
        };
        Assert.False(emitter.CanEmit(rec));
    }

    [Fact]
    public void ReadBootstrap_PrefersBootstrapMetadataKey()
    {
        var step = new BowireRecordingStep
        {
            Metadata = new Dictionary<string, string> { ["bootstrap"] = "b1:9092,b2:9092" }
        };
        Assert.Equal("b1:9092,b2:9092", SurgewaveMockEmitter.ReadBootstrap(step));
    }

    [Fact]
    public void ReadBootstrap_FallsBackToBootstrapServersKey()
    {
        var step = new BowireRecordingStep
        {
            Metadata = new Dictionary<string, string> { ["bootstrap-servers"] = "broker:9094" }
        };
        Assert.Equal("broker:9094", SurgewaveMockEmitter.ReadBootstrap(step));
    }

    [Fact]
    public void ReadBootstrap_DefaultsToLocalhost()
    {
        Assert.Equal("localhost:9092", SurgewaveMockEmitter.ReadBootstrap(new BowireRecordingStep()));
    }

    [Fact]
    public void DecodePayload_PrefersResponseBinary()
    {
        var step = new BowireRecordingStep
        {
            ResponseBinary = Convert.ToBase64String([0xDE, 0xAD]),
            Body = "ignored"
        };
        Assert.Equal(new byte[] { 0xDE, 0xAD },
            SurgewaveMockEmitter.DecodePayload(step, NullLogger.Instance));
    }

    [Fact]
    public void DecodePayload_FallsBackToBodyAsUtf8()
    {
        var step = new BowireRecordingStep { Body = "hi" };
        Assert.Equal(new byte[] { 0x68, 0x69 },
            SurgewaveMockEmitter.DecodePayload(step, NullLogger.Instance));
    }

    [Fact]
    public void DecodePayload_Nothing_ReturnsNull()
    {
        Assert.Null(SurgewaveMockEmitter.DecodePayload(new BowireRecordingStep(), NullLogger.Instance));
    }

    [Fact]
    public void DecodePayload_MalformedBase64_ReturnsNull()
    {
        Assert.Null(SurgewaveMockEmitter.DecodePayload(
            new BowireRecordingStep { ResponseBinary = "not-base64!" }, NullLogger.Instance));
    }
}
