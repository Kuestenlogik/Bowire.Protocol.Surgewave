// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

namespace Kuestenlogik.Bowire.Protocol.Surgewave.Tests;

public sealed class SurgewaveConnectionTests
{
    [Fact]
    public void TryParse_SurgewaveUrl_NormalisesHostPort()
    {
        var endpoint = SurgewaveConnection.TryParse("surgewave://broker.example:9092");
        Assert.NotNull(endpoint);
        Assert.Equal("broker.example:9092", endpoint!.Value.BootstrapServers);
    }

    [Fact]
    public void TryParse_MultipleBrokers_PreservesCsv()
    {
        var endpoint = SurgewaveConnection.TryParse("surgewave://b1:9092,b2:9093");
        Assert.NotNull(endpoint);
        Assert.Equal("b1:9092,b2:9093", endpoint!.Value.BootstrapServers);
    }

    [Fact]
    public void TryParse_BareHostPort_NoScheme_Accepted()
    {
        var endpoint = SurgewaveConnection.TryParse("broker:9092");
        Assert.NotNull(endpoint);
        Assert.Equal("broker:9092", endpoint!.Value.BootstrapServers);
    }

    [Fact]
    public void TryParse_HostWithoutPort_AppliesDefault()
    {
        var endpoint = SurgewaveConnection.TryParse("surgewave://broker.example");
        Assert.NotNull(endpoint);
        Assert.Equal("broker.example:9092", endpoint!.Value.BootstrapServers);
    }

    [Fact]
    public void TryParse_NonSurgewaveScheme_ReturnsNull()
    {
        Assert.Null(SurgewaveConnection.TryParse("https://example.com"));
        Assert.Null(SurgewaveConnection.TryParse("kafka://broker:9092"));
    }

    [Fact]
    public void TryParse_Empty_ReturnsNull()
    {
        Assert.Null(SurgewaveConnection.TryParse(""));
        Assert.Null(SurgewaveConnection.TryParse(null));
    }

    [Fact]
    public void TryParse_EmbeddedHost_IsFlaggedEmbedded()
    {
        var endpoint = SurgewaveConnection.TryParse("surgewave://embedded");
        Assert.NotNull(endpoint);
        Assert.True(endpoint!.Value.IsEmbedded);
        Assert.Equal("embedded", endpoint.Value.BootstrapServers);
    }

    [Fact]
    public void TryParse_EmbeddedHost_CaseInsensitive()
    {
        var endpoint = SurgewaveConnection.TryParse("surgewave://EMBEDDED/");
        Assert.NotNull(endpoint);
        Assert.True(endpoint!.Value.IsEmbedded);
    }

    [Fact]
    public void TryParse_RegularHostNamedEmbedded_NotFlagged()
    {
        // "embedded:9092" explicitly carries a port — treat as a
        // regular broker host coincidentally named "embedded".
        var endpoint = SurgewaveConnection.TryParse("surgewave://embedded:9092");
        Assert.NotNull(endpoint);
        Assert.False(endpoint!.Value.IsEmbedded);
        Assert.Equal("embedded:9092", endpoint.Value.BootstrapServers);
    }
}
