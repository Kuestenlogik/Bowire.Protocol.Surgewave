// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

namespace Kuestenlogik.Bowire.Protocol.Surgewave.Tests;

/// <summary>
/// The two settings this plugin declares reach the code that acts on them.
/// </summary>
/// <remarks>
/// This plugin had an <c>Initialize</c> already — it captures the host's
/// provider for the embedded broker tap — and still read neither setting,
/// which is the more misleading version of the gap DIS closed in
/// Kuestenlogik/Bowire#640: the wiring looked present.
/// </remarks>
public sealed class SurgewavePluginSettingsTests
{
    [Fact]
    public void Without_a_settings_store_the_defaults_stand()
    {
        var plugin = new BowireSurgewaveProtocol();

        Assert.Equal(TimeSpan.FromSeconds(5), plugin.DiscoveryTimeout());
        Assert.StartsWith("bowire-", plugin.NewClientId(), StringComparison.Ordinal);
    }

    [Fact]
    public void The_embedded_tap_provider_is_still_captured_when_it_carries_no_settings()
    {
        var plugin = new BowireSurgewaveProtocol();
        plugin.Initialize(new EmptyServiceProvider());

        Assert.Equal(TimeSpan.FromSeconds(5), plugin.DiscoveryTimeout());
        Assert.StartsWith("bowire-", plugin.NewClientId(), StringComparison.Ordinal);
    }

    [Fact]
    public void The_workspaces_discovery_timeout_is_what_the_probe_is_given()
    {
        var plugin = new BowireSurgewaveProtocol();
        plugin.Initialize(new FakePluginSettings(("discoveryTimeoutSeconds", "12")));

        Assert.Equal(TimeSpan.FromSeconds(12), plugin.DiscoveryTimeout());
    }

    [Fact]
    public void The_workspaces_prefix_names_the_client_a_broker_sees()
    {
        var plugin = new BowireSurgewaveProtocol();
        plugin.Initialize(new FakePluginSettings(("clientIdPrefix", "recon")));

        Assert.StartsWith("recon-", plugin.NewClientId(), StringComparison.Ordinal);
    }

    [Fact]
    public void Two_connections_get_different_client_ids_under_one_prefix()
    {
        var plugin = new BowireSurgewaveProtocol();
        plugin.Initialize(new FakePluginSettings(("clientIdPrefix", "recon")));

        // Two workbench tabs against one broker must not collide.
        Assert.NotEqual(plugin.NewClientId(), plugin.NewClientId());
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void A_blank_prefix_falls_back_rather_than_leaving_a_bare_guid(string configured)
    {
        var plugin = new BowireSurgewaveProtocol();
        plugin.Initialize(new FakePluginSettings(("clientIdPrefix", configured)));

        Assert.StartsWith("bowire-", plugin.NewClientId(), StringComparison.Ordinal);
    }

    private sealed class EmptyServiceProvider : IServiceProvider
    {
        public object? GetService(Type serviceType) => null;
    }
}
