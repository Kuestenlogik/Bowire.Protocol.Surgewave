// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;

namespace Kuestenlogik.Bowire.Protocol.Surgewave;

/// <summary>
/// Reads schemas from a Confluent-wire-compatible Schema Registry over its REST
/// API and decodes wire-format payloads (0x00 magic byte + 4-byte big-endian
/// schema id + body) for the message browser. Loads <c>&lt;topic&gt;-key</c> /
/// <c>&lt;topic&gt;-value</c> subjects and by-id schemas on demand, caching them
/// so per-message decode pays one HTTP round-trip per schema.
/// <para>
/// Surgewave's own schema registry and any Confluent-compatible registry expose
/// the SAME REST surface (<c>GET /schemas/ids/{id}</c>,
/// <c>GET /subjects/{subject}/versions/latest</c>), so browsing typed payloads
/// works against native Surgewave AND a drop-in Confluent registry WITHOUT a
/// hard Confluent library dependency: the fetch is a plain HTTP GET and Avro
/// decoding uses Apache Avro directly. That keeps the native Surgewave plugin
/// free of Confluent so a fully-migrated deployment carries no Confluent
/// baggage — Confluent stays an opt-in adapter, never a mandatory dependency.
/// </para>
/// </summary>
internal sealed class SurgewaveSchemaRegistry : IDisposable
{
    private readonly HttpClient _http;
    private readonly ConcurrentDictionary<int, RegisteredSchema?> _byId = new();
    private readonly ConcurrentDictionary<string, RegisteredSchema?> _bySubject = new(StringComparer.Ordinal);

    public string Url { get; }

    public SurgewaveSchemaRegistry(string url)
    {
        Url = url;
        _http = new HttpClient
        {
            BaseAddress = new Uri(url.TrimEnd('/') + "/"),
            Timeout = TimeSpan.FromSeconds(5),
        };
    }

    /// <summary>A schema fetched from the registry (Confluent-compatible shape).</summary>
    public sealed record RegisteredSchema(int Id, int Version, string SchemaType, string SchemaString);

    /// <summary>
    /// Look up the latest schema registered against <paramref name="subject"/>
    /// (typically <c>&lt;topic&gt;-value</c> / <c>&lt;topic&gt;-key</c>).
    /// Returns <c>null</c> when the subject is missing — schemaless topics are
    /// common in dev clusters and the caller falls back to raw bytes.
    /// </summary>
    public async Task<RegisteredSchema?> TryGetLatestAsync(string subject)
    {
        if (_bySubject.TryGetValue(subject, out var cached)) return cached;
        var schema = await FetchAsync($"subjects/{Uri.EscapeDataString(subject)}/versions/latest").ConfigureAwait(false);
        _bySubject[subject] = schema;
        if (schema is not null) _byId[schema.Id] = schema;
        return schema;
    }

    /// <summary>
    /// Decode a Confluent wire-format payload (<c>0x00</c> magic + 4-byte
    /// big-endian schema id + Avro/JSON body) into a human-readable string.
    /// Returns <c>null</c> when the bytes don't carry the framing prefix (plain
    /// UTF-8 / opaque binary), so the caller falls back to UTF-8 + base64.
    /// </summary>
    public async Task<string?> TryDecodeAsync(byte[]? payload)
    {
        if (payload is null || payload.Length < 5) return null;
        if (payload[0] != 0x00) return null;
        var schemaId = (payload[1] << 24) | (payload[2] << 16) | (payload[3] << 8) | payload[4];
        var body = new byte[payload.Length - 5];
        Buffer.BlockCopy(payload, 5, body, 0, body.Length);

        var schema = await TryGetByIdAsync(schemaId).ConfigureAwait(false);
        if (schema is null) return null;

        return schema.SchemaType.ToUpperInvariant() switch
        {
            // JSON Schema payloads carry the JSON document straight after the
            // prefix; Protobuf isn't decoded yet. Avro is the priority.
            "JSON" => Encoding.UTF8.GetString(body),
            "AVRO" or "" => DecodeAvroBody(body, schema.SchemaString),
            _ => null,
        };
    }

    private async Task<RegisteredSchema?> TryGetByIdAsync(int schemaId)
    {
        if (_byId.TryGetValue(schemaId, out var cached)) return cached;
        var schema = await FetchAsync($"schemas/ids/{schemaId}", forcedId: schemaId).ConfigureAwait(false);
        _byId[schemaId] = schema;
        return schema;
    }

    private async Task<RegisteredSchema?> FetchAsync(string path, int? forcedId = null)
    {
        try
        {
            using var response = await _http.GetAsync(new Uri(path, UriKind.Relative)).ConfigureAwait(false);
            if (!response.IsSuccessStatusCode) return null;

            await using var stream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
            using var doc = await JsonDocument.ParseAsync(stream).ConfigureAwait(false);
            var root = doc.RootElement;

            var schemaString = root.TryGetProperty("schema", out var s) ? s.GetString() ?? "" : "";
            // Confluent omits schemaType for Avro; treat absence as AVRO.
            var schemaType = root.TryGetProperty("schemaType", out var t) ? t.GetString() ?? "AVRO" : "AVRO";
            var id = forcedId ?? (root.TryGetProperty("id", out var i) ? i.GetInt32() : 0);
            var version = root.TryGetProperty("version", out var v) ? v.GetInt32() : 0;
            return new RegisteredSchema(id, version, schemaType, schemaString);
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or JsonException)
        {
            return null;
        }
    }

    private static string? DecodeAvroBody(byte[] body, string schemaJson)
    {
        try
        {
            var schema = Avro.Schema.Parse(schemaJson);
            using var ms = new MemoryStream(body);
            var decoder = new Avro.IO.BinaryDecoder(ms);
            var reader = new Avro.Generic.GenericDatumReader<object>(schema, schema);
            var obj = reader.Read(reuse: null!, decoder);
            return AvroValueToJson.Serialize(obj);
        }
        catch
        {
            return null;
        }
    }

    public void Dispose() => _http.Dispose();
}
