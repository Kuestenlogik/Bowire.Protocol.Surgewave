// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using Kuestenlogik.Surgewave.Client.Abstractions;

namespace Kuestenlogik.Bowire.Protocol.Surgewave.Tests;

/// <summary>
/// Tests for the <c>?protocol=…</c> query-parameter handling on
/// <see cref="SurgewaveConnection.TryParse"/>. Surgewave's broker speaks both
/// the native wire protocol and Kafka-compatible wire — the URL hints
/// drive which one the SurgewaveClient builder selects.
/// </summary>
public sealed class SurgewaveConnectionProtocolTests
{
    [Fact]
    public void TryParse_NoQueryString_DefaultsToAuto()
    {
        var ep = SurgewaveConnection.TryParse("surgewave://broker:9092");
        Assert.Equal(ProtocolType.Auto, ep!.Value.Protocol);
    }

    [Fact]
    public void TryParse_ProtocolSurgewave_PicksSurgewaveNative()
    {
        var ep = SurgewaveConnection.TryParse("surgewave://broker:9092?protocol=surgewave");
        Assert.Equal(ProtocolType.SurgewaveNative, ep!.Value.Protocol);
        Assert.Equal("broker:9092", ep.Value.BootstrapServers);
    }

    [Fact]
    public void TryParse_ProtocolNative_AliasResolvesToSurgewaveNative()
    {
        // "native" is a friendlier alias for the same value — saves
        // users from remembering the exact spelling of "surgewave-native".
        var ep = SurgewaveConnection.TryParse("surgewave://broker:9092?protocol=native");
        Assert.Equal(ProtocolType.SurgewaveNative, ep!.Value.Protocol);
    }

    [Fact]
    public void TryParse_ProtocolSurgewaveNative_AliasResolvesToSurgewaveNative()
    {
        var ep = SurgewaveConnection.TryParse("surgewave://broker:9092?protocol=surgewave-native");
        Assert.Equal(ProtocolType.SurgewaveNative, ep!.Value.Protocol);
    }

    [Fact]
    public void TryParse_ProtocolKafka_PicksKafka()
    {
        var ep = SurgewaveConnection.TryParse("surgewave://broker:9092?protocol=kafka");
        Assert.Equal(ProtocolType.Kafka, ep!.Value.Protocol);
    }

    [Fact]
    public void TryParse_ProtocolAuto_PicksAuto()
    {
        var ep = SurgewaveConnection.TryParse("surgewave://broker:9092?protocol=auto");
        Assert.Equal(ProtocolType.Auto, ep!.Value.Protocol);
    }

    [Fact]
    public void TryParse_ProtocolUnknownValue_FallsBackToAuto()
    {
        // A typo or future-unknown value should be a friendly fallback,
        // not a parse error — the user gets the default behaviour.
        var ep = SurgewaveConnection.TryParse("surgewave://broker:9092?protocol=mqtt");
        Assert.Equal(ProtocolType.Auto, ep!.Value.Protocol);
    }

    [Fact]
    public void TryParse_ProtocolCaseInsensitive()
    {
        var lower = SurgewaveConnection.TryParse("surgewave://broker:9092?protocol=kafka");
        var upper = SurgewaveConnection.TryParse("surgewave://broker:9092?protocol=KAFKA");
        var mixed = SurgewaveConnection.TryParse("surgewave://broker:9092?protocol=Kafka");
        Assert.Equal(ProtocolType.Kafka, lower!.Value.Protocol);
        Assert.Equal(ProtocolType.Kafka, upper!.Value.Protocol);
        Assert.Equal(ProtocolType.Kafka, mixed!.Value.Protocol);
    }

    [Fact]
    public void TryParse_ProtocolWithMultipleBootstrapServers_BothSurvive()
    {
        var ep = SurgewaveConnection.TryParse("surgewave://b1:9092,b2:9092?protocol=kafka");
        Assert.Equal("b1:9092,b2:9092", ep!.Value.BootstrapServers);
        Assert.Equal(ProtocolType.Kafka, ep.Value.Protocol);
    }

    [Fact]
    public void TryParse_EmbeddedUrl_IgnoresProtocolHint()
    {
        // The embedded URL routes to ISurgewaveBrokerObservability — neither
        // wire protocol applies, so any ?protocol= hint is irrelevant.
        var ep = SurgewaveConnection.TryParse("surgewave://embedded?protocol=kafka");
        Assert.True(ep!.Value.IsEmbedded);
    }

    [Fact]
    public void TryParse_BareHostPort_NoQuery_DefaultsToAuto()
    {
        // Bare "host:port" form (no scheme) still picks up Auto.
        var ep = SurgewaveConnection.TryParse("broker:9092");
        Assert.Equal(ProtocolType.Auto, ep!.Value.Protocol);
    }

    [Fact]
    public void TryParse_SchemaRegistry_PicksUrl()
    {
        var ep = SurgewaveConnection.TryParse("surgewave://broker:9092?schema-registry=http://sr:8081");
        Assert.Equal("http://sr:8081", ep!.Value.SchemaRegistryUrl);
    }

    [Fact]
    public void TryParse_SchemaRegistryShortAlias_sr()
    {
        var ep = SurgewaveConnection.TryParse("surgewave://broker:9092?sr=http://sr:8081");
        Assert.Equal("http://sr:8081", ep!.Value.SchemaRegistryUrl);
    }

    [Fact]
    public void TryParse_ProtocolAndSchemaRegistry_BothCarriedThrough()
    {
        // Both query parameters land in the parsed Endpoint — typical
        // user URL when targeting Surgewave's Kafka-compat mode with a
        // schema registry alongside.
        var ep = SurgewaveConnection.TryParse("surgewave://broker:9092?protocol=kafka&schema-registry=http://sr:8081");
        Assert.Equal(ProtocolType.Kafka, ep!.Value.Protocol);
        Assert.Equal("http://sr:8081", ep.Value.SchemaRegistryUrl);
    }

    [Fact]
    public void TryParse_NoSchemaRegistry_LeavesNull()
    {
        var ep = SurgewaveConnection.TryParse("surgewave://broker:9092");
        Assert.Null(ep!.Value.SchemaRegistryUrl);
    }

    [Fact]
    public void TryParse_UrlEncodedSchemaRegistry_Decodes()
    {
        var ep = SurgewaveConnection.TryParse("surgewave://broker:9092?schema-registry=http%3A%2F%2Fsr%3A8081");
        Assert.Equal("http://sr:8081", ep!.Value.SchemaRegistryUrl);
    }
}
