// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Bowire;
using Kuestenlogik.Bowire.Models;
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Core.Observability;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Bowire.Protocol.Surgewave;

/// <summary>
/// Bowire protocol plugin for the native Kuestenlogik.Surgewave messaging protocol.
/// Connects to a Surgewave broker via <see cref="SurgewaveClient"/>, enumerates
/// the advertised topics, and surfaces each as a Bowire service with
/// <c>consume</c> and <c>produce</c> methods. Messages are decoded
/// into the same JSON envelope shape the Kafka plugin emits so the
/// workbench renders Surgewave traffic identically to generic Kafka —
/// the payload can hop between the two plugins without retouching
/// the UI.
/// </summary>
/// <remarks>
/// <para>
/// Why a separate plugin from <c>Bowire.Protocol.Kafka</c>: the Surgewave
/// transport has its own schema-registry conventions,
/// native-protocol encoding, and topic-naming idioms. Users outside
/// the Surgewave ecosystem shouldn't pay for those; Surgewave users shouldn't
/// have to re-implement them on top of a Confluent.Kafka-based
/// plugin. Both plugins can coexist — pick <c>surgewave://</c> URLs for
/// Surgewave clusters, <c>kafka://</c> URLs for generic Kafka.
/// </para>
/// <para>
/// The consume/produce method shape is deliberately identical to the
/// Kafka plugin so recordings produced against one can replay against
/// the other (swap the <c>protocol</c> string on the steps). The mock
/// emitter in this plugin publishes through <see cref="SurgewaveClient"/>
/// so the native-protocol framing round-trips as captured.
/// </para>
/// </remarks>
public sealed class BowireSurgewaveProtocol : IBowireProtocol
{
    /// <summary>Method name of the streaming consume operation.</summary>
    public const string ConsumeMethodName = "consume";

    /// <summary>Method name of the unary produce operation.</summary>
    public const string ProduceMethodName = "produce";

    /// <summary>Synthetic service name used when tapping the host's
    /// in-process Surgewave broker via <c>surgewave://embedded</c>. Every
    /// broker event flows through this single service regardless of
    /// topic, since the tap is cluster-wide.</summary>
    public const string EmbeddedTapServiceName = "BrokerTap";

    /// <summary>Synthetic service for cluster / broker metadata.</summary>
    public const string ClusterServiceName = "Cluster";

    // Captured during Initialize() when Bowire is hosted inside a
    // Surgewave broker process. Used to resolve ISurgewaveBrokerObservability
    // for `surgewave://embedded` URLs — null in standalone Bowire usage.
    private IServiceProvider? _serviceProvider;

    /// <inheritdoc />
    public string Name => "Surgewave";

    /// <inheritdoc />
    public string Id => "surgewave";

    /// <inheritdoc />
    public void Initialize(IServiceProvider? serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }

    /// <inheritdoc />
    // Three diverging streams — evokes the Surgewave project's data-
    // distribution fan-out. Küstenlogik brand palette (blue).
    public string IconSvg => """<svg viewBox="0 0 24 24" fill="none" stroke="#38bdf8" stroke-width="1.5" width="16" height="16" aria-hidden="true"><path d="M4 12h4"/><path d="M8 12 L14 6"/><path d="M8 12 L14 12"/><path d="M8 12 L14 18"/><circle cx="15.5" cy="6" r="1.3" fill="#38bdf8"/><circle cx="15.5" cy="12" r="1.3" fill="#38bdf8"/><circle cx="15.5" cy="18" r="1.3" fill="#38bdf8"/></svg>""";

    /// <inheritdoc />
    public IReadOnlyList<BowirePluginSetting> Settings =>
    [
        new("discoveryTimeoutSeconds", "Discovery timeout",
            "Max seconds to wait on broker metadata during discovery",
            "number", 5),
        new("clientIdPrefix", "Client-id prefix",
            "Prefix used for the generated client id on discovery + streaming",
            "string", "bowire"),
    ];

    /// <inheritdoc />
    public async Task<List<BowireServiceInfo>> DiscoverAsync(
        string serverUrl, bool showInternalServices, CancellationToken ct = default)
    {
        var endpoint = SurgewaveConnection.TryParse(serverUrl);
        if (endpoint is null) return [];

        if (endpoint.Value.IsEmbedded)
            return BuildEmbeddedDiscovery(serverUrl);

        var protocolDescription = endpoint.Value.Protocol switch
        {
            ProtocolType.SurgewaveNative => "Surgewave-native protocol",
            ProtocolType.Kafka => "Kafka-compatible wire protocol",
            _ => "auto-detect (Surgewave-native, falls back to Kafka)",
        };

        var services = new List<BowireServiceInfo>
        {
            new(ClusterServiceName, "surgewave", [])
            {
                Source = "surgewave",
                OriginUrl = serverUrl,
                Description = $"Surgewave broker on {endpoint.Value.BootstrapServers} via {protocolDescription}.",
            },
        };

        // Topic enumeration on the native Surgewave protocol — the
        // Kuestenlogik.Surgewave.Client SDK is still rounding out its admin surface.
        // Today we can't reliably list topics via the native wire
        // without a recorded broker handshake, so we return the
        // cluster service + a best-effort placeholder the user can
        // override by typing the topic name into the workbench's
        // method dropdown. Once Kuestenlogik.Surgewave.Client gains a metadata API
        // this branch fills in topics the same way the Kafka plugin
        // does via IAdminClient.GetMetadata.
        try
        {
            // DiscoverAsync has no metadata parameter today — auth markers
            // can't ride here. Once IBowireProtocol gains metadata-aware
            // discovery, the same SurgewaveSecurityConfig path kicks in.
            await using var client = await BuildSurgewaveClientAsync(endpoint.Value, metadata: null, ct);
            _ = client.IsConnected; // probe; no-op on fake URLs, throws on real connection errors
        }
        catch (Exception)
        {
            // Broker unreachable — return the cluster service but
            // nothing else. Discovery never crashes the sidebar.
        }

        return services;
    }

    /// <summary>
    /// Build a <see cref="ISurgewaveClient"/> from the parsed endpoint,
    /// honouring the URL's <c>?protocol=…</c> hint and the Bowire
    /// auth markers in <paramref name="metadata"/>. <see cref="ProtocolType.Auto"/>
    /// uses the SDK default (try Surgewave-native, fall back to Kafka).
    /// </summary>
    private static Task<ISurgewaveClient> BuildSurgewaveClientAsync(
        SurgewaveConnection.Endpoint endpoint,
        IReadOnlyDictionary<string, string>? metadata,
        CancellationToken ct)
    {
        var builder = SurgewaveClient.Create(endpoint.BootstrapServers);
        builder = endpoint.Protocol switch
        {
            ProtocolType.SurgewaveNative => builder.UseSurgewaveProtocol(),
            ProtocolType.Kafka => builder.UseKafkaProtocol(),
            _ => builder.UseAutoDetect(),
        };

        // Pull mTLS / SASL markers out of metadata and onto the builder.
        // The sanitised dict drops both markers so the produce path
        // doesn't accidentally forward them as Kafka headers.
        (builder, _) = SurgewaveSecurityConfig.Apply(builder, metadata);

        return builder.BuildAsync(ct);
    }

    /// <summary>
    /// Returns a copy of <paramref name="metadata"/> with the security
    /// markers stripped — what the produce/consume path sees on the
    /// wire as Kafka headers.
    /// </summary>
    private static Dictionary<string, string>? StripSecurityMarkers(Dictionary<string, string>? metadata)
    {
        if (metadata is null) return null;
        var copy = new Dictionary<string, string>(metadata.Count, StringComparer.Ordinal);
        foreach (var (k, v) in metadata)
        {
            if (string.Equals(k, Kuestenlogik.Bowire.Auth.MtlsConfig.MtlsMarkerKey, StringComparison.Ordinal)) continue;
            if (string.Equals(k, SurgewaveSecurityConfig.SaslMarkerKey, StringComparison.Ordinal)) continue;
            copy[k] = v;
        }
        return copy;
    }

    /// <inheritdoc />
    public async Task<InvokeResult> InvokeAsync(
        string serverUrl, string service, string method,
        List<string> jsonMessages, bool showInternalServices,
        Dictionary<string, string>? metadata = null, CancellationToken ct = default)
    {
        var endpoint = SurgewaveConnection.TryParse(serverUrl);
        if (endpoint is null)
        {
            return new InvokeResult(
                null, 0, "Invalid Surgewave broker URL.",
                new Dictionary<string, string>());
        }

        if (!string.Equals(method, ProduceMethodName, StringComparison.OrdinalIgnoreCase))
        {
            return new InvokeResult(
                null, 0,
                "Surgewave invocation only supports 'produce'. Open the 'consume' stream to observe topic traffic.",
                new Dictionary<string, string>());
        }

        var payload = jsonMessages.FirstOrDefault() ?? "{}";

        // Pull mTLS / SASL markers off metadata before reading "key"
        // and "partition" — the security helpers consume them and the
        // sanitised dict is what the produce path forwards as headers.
        var sanitisedMetadata = StripSecurityMarkers(metadata);
        var key = sanitisedMetadata?.TryGetValue("key", out var k) == true ? k : null;

        var sw = System.Diagnostics.Stopwatch.StartNew();
        await using var client = await BuildSurgewaveClientAsync(endpoint.Value, metadata, ct);
        var producer = client.CreateProducer<string?, byte[]>();

        try
        {
            var valueBytes = Encoding.UTF8.GetBytes(payload);
            ProduceResult result;
            if (sanitisedMetadata?.TryGetValue("partition", out var partitionStr) == true &&
                int.TryParse(partitionStr, System.Globalization.NumberStyles.Integer,
                    System.Globalization.CultureInfo.InvariantCulture, out var partition))
            {
                result = await producer.ProduceAsync(service, partition, key, valueBytes, ct);
            }
            else
            {
                result = await producer.ProduceAsync(service, key, valueBytes, ct);
            }
            sw.Stop();

            var responseMeta = new Dictionary<string, string>
            {
                ["topic"] = result.Topic,
                ["partition"] = result.Partition.ToString(System.Globalization.CultureInfo.InvariantCulture),
                ["offset"] = result.Offset.ToString(System.Globalization.CultureInfo.InvariantCulture),
            };
            return new InvokeResult(
                JsonSerializer.Serialize(new
                {
                    topic = result.Topic,
                    partition = result.Partition,
                    offset = result.Offset,
                    bytes = Encoding.UTF8.GetByteCount(payload),
                }),
                sw.ElapsedMilliseconds,
                "OK",
                responseMeta);
        }
        catch (Exception ex)
        {
            sw.Stop();
            return new InvokeResult(
                null, sw.ElapsedMilliseconds,
                "Error: " + ex.Message,
                new Dictionary<string, string>());
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<string> InvokeStreamAsync(
        string serverUrl, string service, string method,
        List<string> jsonMessages, bool showInternalServices,
        Dictionary<string, string>? metadata = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var endpoint = SurgewaveConnection.TryParse(serverUrl);
        if (endpoint is null) yield break;
        if (!string.Equals(method, ConsumeMethodName, StringComparison.OrdinalIgnoreCase)) yield break;

        // `surgewave://embedded` routes to the in-process observability
        // feed instead of opening a TCP consumer. Every broker event
        // (produce/consume/reject/rebalance) is forwarded as a tap
        // envelope — no topic subscription, the stream is cluster-wide.
        if (endpoint.Value.IsEmbedded)
        {
            await foreach (var env in StreamEmbeddedAsync(ct).ConfigureAwait(false))
                yield return env;
            yield break;
        }

        ISurgewaveClient? client = null;
        Kuestenlogik.Surgewave.Client.Abstractions.IConsumer<byte[]?, byte[]>? consumer = null;
        try
        {
            client = await BuildSurgewaveClientAsync(endpoint.Value, metadata, ct);
            consumer = client.CreateConsumer<byte[]?, byte[]>();
            await consumer.SubscribeAsync(ct, service);
        }
        catch (Exception)
        {
            // Broker connect / subscribe failed — surface an empty
            // stream so the UI can show "no traffic" rather than
            // erroring out on the whole pane.
            if (consumer is not null) try { await consumer.DisposeAsync(); } catch { /* best-effort */ }
            if (client is not null) try { await client.DisposeAsync(); } catch { /* best-effort */ }
            yield break;
        }

        // Optional Schema Registry — held open for the duration of the
        // stream so per-message decode pays the HTTP cost only once per
        // schema id (the registry client caches internally). Only
        // meaningful in the Kafka-wire mode; the native protocol carries
        // its own typed envelope already.
        using var registry = endpoint.Value.SchemaRegistryUrl is { Length: > 0 } srUrl
            ? new SurgewaveSchemaRegistry(srUrl)
            : null;

        try
        {
            while (!ct.IsCancellationRequested)
            {
                Kuestenlogik.Surgewave.Client.Consumer.ConsumeResult<byte[]?, byte[]>? result;
                try
                {
                    result = await consumer.ConsumeAsync(ct);
                }
                catch (OperationCanceledException) { yield break; }
                catch (Exception) { yield break; }

                if (result is null) continue;
                yield return await BuildEnvelopeAsync(result, registry).ConfigureAwait(false);
            }
        }
        finally
        {
            try { await consumer.DisposeAsync(); } catch { /* best-effort */ }
            try { await client.DisposeAsync(); } catch { /* best-effort */ }
        }
    }

    /// <inheritdoc />
    public Task<IBowireChannel?> OpenChannelAsync(
        string serverUrl, string service, string method,
        bool showInternalServices, Dictionary<string, string>? metadata = null,
        CancellationToken ct = default)
        => Task.FromResult<IBowireChannel?>(null);

    /// <summary>
    /// Render a consumed Surgewave message as a Bowire stream envelope.
    /// Matches the Kafka plugin's envelope shape (topic, partition,
    /// offset, key, value, base64 fallbacks) so downstream UI doesn't
    /// need to learn two shapes.
    /// </summary>
    internal static string BuildEnvelope(
        Kuestenlogik.Surgewave.Client.Consumer.ConsumeResult<byte[]?, byte[]> result)
    {
        return BuildEnvelopeAsync(result, registry: null).GetAwaiter().GetResult();
    }

    /// <summary>
    /// Schema-Registry-aware envelope builder. When <paramref name="registry"/>
    /// is non-null and the message body carries the Confluent wire format
    /// (magic byte 0x00 + 4-byte schema id), the value is decoded to a
    /// JSON projection and tagged with <c>encoding</c> so the UI renders
    /// the typed shape instead of a base64 blob. Kafka-mode only — the
    /// native protocol's payload is already typed.
    /// </summary>
    internal static async Task<string> BuildEnvelopeAsync(
        Kuestenlogik.Surgewave.Client.Consumer.ConsumeResult<byte[]?, byte[]> result,
        SurgewaveSchemaRegistry? registry)
    {
        var keyBytes = result.Key;
        var valueBytes = result.Value ?? [];
        string? keyText = TryDecodeUtf8(keyBytes);
        string? valueText = TryDecodeUtf8(valueBytes);
        string? encoding = null;

        if (registry is not null)
        {
            var decoded = await registry.TryDecodeAsync(valueBytes).ConfigureAwait(false);
            if (decoded is not null)
            {
                valueText = decoded;
                encoding = "avro";
            }
        }

        var envelope = new
        {
            topic = result.Topic,
            partition = result.Partition,
            offset = result.Offset,
            timestamp = result.Timestamp.ToUnixTimeMilliseconds(),
            key = keyText,
            keyBase64 = keyBytes is null ? null : Convert.ToBase64String(keyBytes),
            value = valueText,
            valueBase64 = Convert.ToBase64String(valueBytes),
            bytes = valueBytes.Length,
            encoding,
        };
        return JsonSerializer.Serialize(envelope);
    }

    internal static string? TryDecodeUtf8(byte[]? bytes)
    {
        if (bytes is null) return null;
        if (bytes.Length == 0) return string.Empty;
        var encoding = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);
        try
        {
            return encoding.GetString(bytes);
        }
        catch (DecoderFallbackException)
        {
            return null;
        }
    }

    /// <summary>
    /// Build the synthetic discovery tree for <c>surgewave://embedded</c>.
    /// One cluster metadata service plus a single <c>BrokerTap</c>
    /// service whose <c>consume</c> method streams every broker
    /// observability event.
    /// </summary>
    private List<BowireServiceInfo> BuildEmbeddedDiscovery(string serverUrl)
    {
        var tapAvailable = _serviceProvider?.GetService<ISurgewaveBrokerObservability>() is not null;
        var description = tapAvailable
            ? "In-process Surgewave broker tap — streams every Produce/Consume/Reject/Rebalance event."
            : "In-process Surgewave broker tap (no ISurgewaveBrokerObservability registered — stream will be empty).";

        return
        [
            new(ClusterServiceName, "surgewave", [])
            {
                Source = "surgewave",
                OriginUrl = serverUrl,
                Description = "In-process Surgewave broker (embedded).",
            },
            new(EmbeddedTapServiceName, "surgewave", [BuildEmbeddedConsumeMethod()])
            {
                Source = "surgewave",
                OriginUrl = serverUrl,
                Description = description,
            },
        ];
    }

    private static BowireMethodInfo BuildEmbeddedConsumeMethod() =>
        new(
            Name: ConsumeMethodName,
            FullName: $"surgewave/{EmbeddedTapServiceName}/{ConsumeMethodName}",
            ClientStreaming: false,
            ServerStreaming: true,
            InputType: new BowireMessageInfo("SurgewaveTapRequest", "surgewave.TapRequest", []),
            OutputType: new BowireMessageInfo("SurgewaveTapEvent", "surgewave.TapEvent", []),
            MethodType: "ServerStreaming")
        {
            Summary = "Observe the in-process broker",
            Description = "Streams every broker event (produced / consumed / rejected / rebalanced) as it happens.",
        };

    /// <summary>
    /// Drive the <see cref="ISurgewaveBrokerObservability"/> feed (when
    /// Bowire is hosted inside a broker process) and yield one JSON
    /// tap envelope per event. Falls through to an empty stream when
    /// no observability service is registered — same UX as "topic with
    /// no traffic" in the live-broker path.
    /// </summary>
    private async IAsyncEnumerable<string> StreamEmbeddedAsync(
        [EnumeratorCancellation] CancellationToken ct)
    {
        var obs = _serviceProvider?.GetService<ISurgewaveBrokerObservability>();
        if (obs is null) yield break;

        IAsyncEnumerable<SurgewaveBrokerEvent> events;
        try
        {
            events = obs.ObserveAsync(ct);
        }
        catch (Exception)
        {
            yield break;
        }

        await foreach (var ev in events.WithCancellation(ct).ConfigureAwait(false))
            yield return BuildTapEnvelope(ev);
    }

    /// <summary>
    /// Render a <see cref="SurgewaveBrokerEvent"/> as the JSON envelope
    /// shape the workbench renders in the stream pane. Superset of the
    /// Kafka envelope — adds the <c>event</c> kind plus broker-scoped
    /// fields (principal, reject reason, rebalance consumers).
    /// </summary>
    internal static string BuildTapEnvelope(SurgewaveBrokerEvent ev)
    {
        var keyBytes = ev.Key;
        var valueBytes = ev.Value ?? [];
        string? keyText = TryDecodeUtf8(keyBytes);
        string? valueText = TryDecodeUtf8(valueBytes);
        var envelope = new
        {
            @event = EventKindToWire(ev.Kind),
            topic = ev.Topic,
            partition = ev.Partition,
            offset = ev.Offset,
            timestamp = ev.Timestamp.ToUnixTimeMilliseconds(),
            principal = ev.Principal,
            reason = ev.RejectReason,
            consumers = ev.Consumers,
            key = keyText,
            keyBase64 = keyBytes is null ? null : Convert.ToBase64String(keyBytes),
            value = valueText,
            valueBase64 = valueBytes.Length == 0 ? null : Convert.ToBase64String(valueBytes),
            bytes = valueBytes.Length,
        };
        return JsonSerializer.Serialize(envelope);
    }

    private static string EventKindToWire(SurgewaveBrokerEventKind kind) => kind switch
    {
        SurgewaveBrokerEventKind.Produced => "produced",
        SurgewaveBrokerEventKind.Consumed => "consumed",
        SurgewaveBrokerEventKind.Rejected => "rejected",
        SurgewaveBrokerEventKind.Rebalanced => "rebalanced",
        _ => "unknown",
    };
}
