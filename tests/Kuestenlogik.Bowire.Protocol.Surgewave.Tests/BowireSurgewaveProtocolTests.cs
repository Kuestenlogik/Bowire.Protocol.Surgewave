// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Runtime.CompilerServices;
using System.Text.Json;
using Kuestenlogik.Surgewave.Core.Observability;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Bowire.Protocol.Surgewave.Tests;

public sealed class BowireSurgewaveProtocolTests
{
    [Fact]
    public async Task Discover_WithMalformedUrl_ReturnsEmpty()
    {
        var plugin = new BowireSurgewaveProtocol();
        var services = await plugin.DiscoverAsync("http://example.com", false, ct: TestContext.Current.CancellationToken);
        Assert.Empty(services);
    }

    [Fact]
    public async Task InvokeAsync_WithConsumeMethod_ReturnsHelpfulError()
    {
        var plugin = new BowireSurgewaveProtocol();
        var result = await plugin.InvokeAsync(
            "surgewave://broker:9092", "orders", BowireSurgewaveProtocol.ConsumeMethodName,
            ["{}"], false, ct: TestContext.Current.CancellationToken);
        Assert.Contains("consume", result.Status, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task InvokeAsync_WithInvalidUrl_ReturnsError()
    {
        var plugin = new BowireSurgewaveProtocol();
        var result = await plugin.InvokeAsync(
            "https://nope", "orders", BowireSurgewaveProtocol.ProduceMethodName, ["{}"], false, ct: TestContext.Current.CancellationToken);
        Assert.Contains("Invalid", result.Status, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TryDecodeUtf8_EdgeCases()
    {
        Assert.Null(BowireSurgewaveProtocol.TryDecodeUtf8(null));
        Assert.Equal(string.Empty, BowireSurgewaveProtocol.TryDecodeUtf8([]));
        Assert.Equal("hi", BowireSurgewaveProtocol.TryDecodeUtf8([0x68, 0x69]));
        Assert.Null(BowireSurgewaveProtocol.TryDecodeUtf8([0xFF])); // invalid UTF-8 lead byte
    }

    [Fact]
    public void IdentityProperties_MatchBowireConventions()
    {
        var plugin = new BowireSurgewaveProtocol();
        Assert.Equal("surgewave", plugin.Id);
        Assert.Equal("Surgewave", plugin.Name);
        Assert.Contains("svg", plugin.IconSvg, StringComparison.Ordinal);
    }

    [Fact]
    public void Settings_ExposesExpectedKnobs()
    {
        var plugin = new BowireSurgewaveProtocol();
        var keys = plugin.Settings.Select(s => s.Key).ToHashSet(StringComparer.Ordinal);
        Assert.Contains("discoveryTimeoutSeconds", keys);
        Assert.Contains("clientIdPrefix", keys);
    }

    [Fact]
    public async Task Discover_Embedded_YieldsClusterAndTapServices()
    {
        var plugin = new BowireSurgewaveProtocol();
        plugin.Initialize(BuildServicesWithTap(new FakeObservability()));

        var services = await plugin.DiscoverAsync("surgewave://embedded", false, ct: TestContext.Current.CancellationToken);

        Assert.Equal(2, services.Count);
        Assert.Contains(services, s => s.Name == BowireSurgewaveProtocol.ClusterServiceName);
        var tap = Assert.Single(services, s => s.Name == BowireSurgewaveProtocol.EmbeddedTapServiceName);
        Assert.Single(tap.Methods);
        Assert.True(tap.Methods[0].ServerStreaming);
        Assert.Equal(BowireSurgewaveProtocol.ConsumeMethodName, tap.Methods[0].Name);
    }

    [Fact]
    public async Task InvokeStream_Embedded_ForwardsObservabilityEvents()
    {
        var fake = new FakeObservability();
        fake.Enqueue(new SurgewaveBrokerEvent(
            SurgewaveBrokerEventKind.Produced, "orders", 0, 42L,
            Principal: "alice", RejectReason: null, Consumers: null,
            Key: null, Value: System.Text.Encoding.UTF8.GetBytes("hi"),
            Timestamp: DateTimeOffset.UnixEpoch));
        fake.Enqueue(new SurgewaveBrokerEvent(
            SurgewaveBrokerEventKind.Rejected, "orders", 0, null,
            Principal: "bob", RejectReason: "acl-deny", Consumers: null,
            Key: null, Value: null,
            Timestamp: DateTimeOffset.UnixEpoch));
        fake.Complete();

        var plugin = new BowireSurgewaveProtocol();
        plugin.Initialize(BuildServicesWithTap(fake));

        var envelopes = new List<string>();
        await foreach (var e in plugin.InvokeStreamAsync(
            "surgewave://embedded", BowireSurgewaveProtocol.EmbeddedTapServiceName,
            BowireSurgewaveProtocol.ConsumeMethodName, [], false, ct: TestContext.Current.CancellationToken))
        {
            envelopes.Add(e);
        }

        Assert.Equal(2, envelopes.Count);
        using var first = JsonDocument.Parse(envelopes[0]);
        Assert.Equal("produced", first.RootElement.GetProperty("event").GetString());
        Assert.Equal("alice", first.RootElement.GetProperty("principal").GetString());
        Assert.Equal("hi", first.RootElement.GetProperty("value").GetString());

        using var second = JsonDocument.Parse(envelopes[1]);
        Assert.Equal("rejected", second.RootElement.GetProperty("event").GetString());
        Assert.Equal("acl-deny", second.RootElement.GetProperty("reason").GetString());
    }

    [Fact]
    public async Task InvokeStream_Embedded_WithoutObservabilityService_YieldsEmpty()
    {
        var plugin = new BowireSurgewaveProtocol();
        plugin.Initialize(new ServiceCollection().BuildServiceProvider());

        var count = 0;
        await foreach (var _ in plugin.InvokeStreamAsync(
            "surgewave://embedded", BowireSurgewaveProtocol.EmbeddedTapServiceName,
            BowireSurgewaveProtocol.ConsumeMethodName, [], false, ct: TestContext.Current.CancellationToken))
        {
            count++;
        }
        Assert.Equal(0, count);
    }

    [Fact]
    public void BuildTapEnvelope_Rebalanced_ShapesConsumersArray()
    {
        var ev = new SurgewaveBrokerEvent(
            SurgewaveBrokerEventKind.Rebalanced, "orders", -1, null,
            Principal: null, RejectReason: null,
            Consumers: ["group-a", "group-b"], Key: null, Value: null,
            Timestamp: DateTimeOffset.UnixEpoch);

        var json = BowireSurgewaveProtocol.BuildTapEnvelope(ev);

        using var doc = JsonDocument.Parse(json);
        Assert.Equal("rebalanced", doc.RootElement.GetProperty("event").GetString());
        var consumers = doc.RootElement.GetProperty("consumers");
        Assert.Equal(2, consumers.GetArrayLength());
        Assert.Equal("group-a", consumers[0].GetString());
    }

    private static ServiceProvider BuildServicesWithTap(ISurgewaveBrokerObservability obs)
    {
        var services = new ServiceCollection();
        services.AddSingleton(obs);
        return services.BuildServiceProvider();
    }

    private sealed class FakeObservability : ISurgewaveBrokerObservability
    {
        private readonly System.Threading.Channels.Channel<SurgewaveBrokerEvent> _channel =
            System.Threading.Channels.Channel.CreateUnbounded<SurgewaveBrokerEvent>();

        // The plugin's hot path checks this before allocating an event;
        // returning true keeps every Enqueue visible regardless of the
        // observability layer's subscriber state.
        public bool HasSubscribers => true;

        public void Enqueue(SurgewaveBrokerEvent ev) => _channel.Writer.TryWrite(ev);
        public void Complete() => _channel.Writer.TryComplete();

        public async IAsyncEnumerable<SurgewaveBrokerEvent> ObserveAsync(
            [EnumeratorCancellation] CancellationToken ct = default)
        {
            while (await _channel.Reader.WaitToReadAsync(ct).ConfigureAwait(false))
            {
                while (_channel.Reader.TryRead(out var ev)) yield return ev;
            }
        }
    }
}
