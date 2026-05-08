// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Text.Json;
using Kuestenlogik.Bowire.Auth;
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Security;

namespace Kuestenlogik.Bowire.Protocol.Surgewave;

/// <summary>
/// Pulls Bowire auth markers out of the request metadata and applies
/// them to a <see cref="SurgewaveClientBuilder"/>. Two markers feed the
/// kafka-mode security knobs:
/// <list type="bullet">
/// <item>
///   <c>__bowireMtls__</c> — shared with REST / gRPC / WebSocket /
///   SignalR / Kafka. PEM cert + key + optional CA / passphrase /
///   allow-self-signed. Maps to
///   <c>SurgewaveClientBuilder.WithSslPem(...)</c>.
/// </item>
/// <item>
///   <c>__bowireKafkaSasl__</c> — the same Kafka-specific JSON
///   <c>{ mechanism, username, password }</c> the Bowire.Protocol.Kafka
///   plugin reads. Surgewave's broker speaks the same Kafka SASL handshake
///   on its compat wire, so the marker shape is shared verbatim.
///   Maps to <c>SurgewaveClientBuilder.WithSasl(mechanism, user, pass)</c>.
/// </item>
/// </list>
/// </summary>
internal static class SurgewaveSecurityConfig
{
    /// <summary>Magic metadata key for SASL credentials. Identical to the Kafka plugin's.</summary>
    public const string SaslMarkerKey = "__bowireKafkaSasl__";

    /// <summary>
    /// Apply the markers to <paramref name="builder"/> and return a
    /// sanitised metadata copy with the markers stripped — anything
    /// passed back to <see cref="IBowireProtocol"/> as Kafka headers
    /// must not contain secrets.
    /// </summary>
    public static (SurgewaveClientBuilder Builder, Dictionary<string, string>? Sanitised) Apply(
        SurgewaveClientBuilder builder, IReadOnlyDictionary<string, string>? metadata)
    {
        if (metadata is null || metadata.Count == 0) return (builder, null);

        var mtls = MtlsConfig.TryParseFromMetadata(metadata);
        if (mtls is not null)
        {
            builder = builder.WithSslPem(
                certificatePem: mtls.CertificatePem,
                privateKeyPem: mtls.PrivateKeyPem,
                passphrase: string.IsNullOrEmpty(mtls.Passphrase) ? null : mtls.Passphrase,
                caCertificatePem: string.IsNullOrEmpty(mtls.CaCertificatePem) ? null : mtls.CaCertificatePem,
                allowSelfSigned: mtls.AllowSelfSigned);
        }

        SaslConfig? sasl = null;
        if (metadata.TryGetValue(SaslMarkerKey, out var saslJson))
        {
            sasl = SaslConfig.TryParse(saslJson);
        }
        if (sasl is not null)
        {
            var mechanism = sasl.Mechanism switch
            {
                "PLAIN" => SaslMechanism.Plain,
                "SCRAM-SHA-256" => SaslMechanism.ScramSha256,
                "SCRAM-SHA-512" => SaslMechanism.ScramSha512,
                "OAUTHBEARER" => SaslMechanism.OAuthBearer,
                _ => SaslMechanism.Plain,
            };
            builder = builder.WithSasl(mechanism, sasl.Username, sasl.Password);
        }

        // Strip both markers — leaking them as Kafka request headers
        // would betray the entire point of the magic-prefix convention.
        var sanitised = new Dictionary<string, string>(metadata.Count, StringComparer.Ordinal);
        foreach (var (k, v) in metadata)
        {
            if (string.Equals(k, MtlsConfig.MtlsMarkerKey, StringComparison.Ordinal)) continue;
            if (string.Equals(k, SaslMarkerKey, StringComparison.Ordinal)) continue;
            sanitised[k] = v;
        }
        return (builder, sanitised);
    }

    /// <summary>
    /// Parsed SASL credentials carried inline in the metadata dict via
    /// <see cref="SaslMarkerKey"/>. Identical wire shape to the Kafka
    /// plugin's KafkaSecurityConfig.SaslConfig — shared on purpose so
    /// users configure SASL once and both plugins consume it.
    /// </summary>
    internal sealed record SaslConfig(string Mechanism, string Username, string Password)
    {
        public static SaslConfig? TryParse(string json)
        {
            try
            {
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;
                if (root.ValueKind != JsonValueKind.Object) return null;

                string? Get(string name) =>
                    root.TryGetProperty(name, out var p) && p.ValueKind == JsonValueKind.String
                        ? p.GetString()
                        : null;

                var mechanism = (Get("mechanism") ?? "PLAIN").ToUpperInvariant();
                var username = Get("username");
                var password = Get("password");
                if (string.IsNullOrEmpty(username) || password is null) return null;

                return new SaslConfig(mechanism, username, password);
            }
            catch (JsonException)
            {
                return null;
            }
        }
    }
}
