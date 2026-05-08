// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Globalization;
using Kuestenlogik.Surgewave.Client.Abstractions;

namespace Kuestenlogik.Bowire.Protocol.Surgewave;

/// <summary>
/// URL parser for Bowire's Surgewave plugin. Accepts the
/// <c>surgewave://</c> scheme (native Surgewave protocol via the Kuestenlogik.Surgewave.Client
/// SDK) and a bare <c>host:port</c> CSV that matches Kafka's
/// bootstrap-servers shape so users can paste the same config they'd
/// use in a Surgewave producer/consumer.
/// </summary>
/// <remarks>
/// The reserved host <c>embedded</c> (i.e. <c>surgewave://embedded</c>)
/// signals an in-process broker observed via the
/// <c>ISurgewaveBrokerObservability</c> service resolved from the host's
/// DI container — no TCP hop. Used when Bowire is hosted inside the
/// same process as a Surgewave broker for interactive debugging.
/// </remarks>
internal static class SurgewaveConnection
{
    /// <summary>Default Surgewave broker port when the URL doesn't specify one.</summary>
    public const int DefaultPort = 9092;

    /// <summary>
    /// Reserved host name that switches the plugin into embedded
    /// observability mode (resolve <c>ISurgewaveBrokerObservability</c>
    /// from DI instead of opening a TCP connection).
    /// </summary>
    public const string EmbeddedHost = "embedded";

    /// <summary>
    /// Parsed broker coordinates. <see cref="IsEmbedded"/> indicates
    /// that the URL pointed at the in-process broker rather than a
    /// TCP endpoint. <see cref="Protocol"/> mirrors Kuestenlogik.Surgewave.Client's
    /// <see cref="ProtocolType"/> so the workbench can toggle between
    /// Surgewave-native, Kafka-wire-compat, and auto-detect against the
    /// same Surgewave broker — Surgewave's broker speaks both protocols.
    /// </summary>
    public readonly record struct Endpoint(
        string BootstrapServers,
        bool IsEmbedded = false,
        ProtocolType Protocol = ProtocolType.Auto,
        string? SchemaRegistryUrl = null);

    /// <summary>
    /// Parse <paramref name="serverUrl"/> as
    /// <c>surgewave://host:port[,host2:port2,...][?protocol=surgewave|kafka|auto]</c>
    /// (or the bare <c>host:port</c> form) and return the canonical
    /// bootstrap-servers CSV plus the requested wire protocol. The
    /// reserved form <c>surgewave://embedded</c> returns an endpoint with
    /// <see cref="Endpoint.IsEmbedded"/> set; the <c>protocol</c>
    /// query parameter is ignored on embedded URLs because the in-process
    /// observability tap doesn't go through either wire format.
    /// Returns <c>null</c> when the URL doesn't look like a Surgewave address.
    /// </summary>
    public static Endpoint? TryParse(string? serverUrl)
    {
        if (string.IsNullOrWhiteSpace(serverUrl)) return null;

        var trimmed = serverUrl.TrimStart();
        if (trimmed.StartsWith("surgewave://", StringComparison.OrdinalIgnoreCase))
            trimmed = trimmed["surgewave://".Length..];
        else if (trimmed.Contains("://", StringComparison.Ordinal))
            return null; // some other scheme — not Surgewave.

        // Pull the query string off before splitting hosts so a comma
        // in a future query value can't break the bootstrap parser.
        var protocol = ProtocolType.Auto;
        string? schemaRegistry = null;
        var queryIdx = trimmed.IndexOf('?', StringComparison.Ordinal);
        if (queryIdx >= 0)
        {
            var query = trimmed[(queryIdx + 1)..];
            protocol = ExtractProtocol(query) ?? ProtocolType.Auto;
            schemaRegistry = ExtractSchemaRegistry(query);
            trimmed = trimmed[..queryIdx];
        }

        trimmed = trimmed.TrimEnd('/');
        if (string.IsNullOrEmpty(trimmed)) return null;

        // Embedded sentinel. Accepts `surgewave://embedded` and
        // `surgewave://embedded/` (trailing slash already stripped above).
        // A port suffix is ignored — `embedded` has no network peer.
        if (string.Equals(trimmed, EmbeddedHost, StringComparison.OrdinalIgnoreCase))
            return new Endpoint(EmbeddedHost, IsEmbedded: true);

        var parts = trimmed.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (parts.Length == 0) return null;

        var normalised = new List<string>(parts.Length);
        foreach (var part in parts)
        {
            var host = part;
            var port = DefaultPort;
            var colon = part.LastIndexOf(':');
            if (colon > 0)
            {
                host = part[..colon];
                var portStr = part[(colon + 1)..];
                if (int.TryParse(portStr, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
                    port = parsed;
            }
            if (string.IsNullOrEmpty(host)) continue;
            normalised.Add(host + ":" + port.ToString(CultureInfo.InvariantCulture));
        }
        if (normalised.Count == 0) return null;

        return new Endpoint(
            string.Join(",", normalised),
            Protocol: protocol,
            SchemaRegistryUrl: schemaRegistry);
    }

    /// <summary>
    /// Pluck the <c>schema-registry</c> entry out of the query-string
    /// portion of the URL. Only meaningful in the Kafka-wire mode —
    /// Surgewave's broker speaks Confluent wire format alongside Kafka, so
    /// the same registry URL works against both. Accepts the short
    /// alias <c>sr</c> too.
    /// </summary>
    private static string? ExtractSchemaRegistry(string query)
    {
        if (string.IsNullOrEmpty(query)) return null;
        foreach (var pair in query.Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            var eq = pair.IndexOf('=', StringComparison.Ordinal);
            if (eq <= 0) continue;
            var key = pair[..eq];
            var value = pair[(eq + 1)..];
            if (string.Equals(key, "schema-registry", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(key, "sr", StringComparison.OrdinalIgnoreCase))
            {
                return Uri.UnescapeDataString(value);
            }
        }
        return null;
    }

    /// <summary>
    /// Pluck the <c>protocol</c> entry out of the query-string portion
    /// of the URL. Bowire only knows three values — anything else
    /// returns null and the caller falls back to <see cref="ProtocolType.Auto"/>.
    /// </summary>
    private static ProtocolType? ExtractProtocol(string query)
    {
        if (string.IsNullOrEmpty(query)) return null;
        foreach (var pair in query.Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            var eq = pair.IndexOf('=', StringComparison.Ordinal);
            if (eq <= 0) continue;
            var key = pair[..eq];
            var value = pair[(eq + 1)..];
            if (!string.Equals(key, "protocol", StringComparison.OrdinalIgnoreCase)) continue;

            if (string.Equals(value, "surgewave", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(value, "native", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(value, "surgewave-native", StringComparison.OrdinalIgnoreCase))
                return ProtocolType.SurgewaveNative;
            if (string.Equals(value, "kafka", StringComparison.OrdinalIgnoreCase))
                return ProtocolType.Kafka;
            if (string.Equals(value, "auto", StringComparison.OrdinalIgnoreCase))
                return ProtocolType.Auto;
            return null;
        }
        return null;
    }
}
