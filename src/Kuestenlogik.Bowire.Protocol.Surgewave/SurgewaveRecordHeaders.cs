// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Text;

namespace Kuestenlogik.Bowire.Protocol.Surgewave;

/// <summary>
/// The record headers of a produce, and the headers a consumed record
/// carries (#86). Produce used to drop every metadata entry but <c>key</c>
/// and <c>partition</c>, though its comments said the rest went out as
/// headers. The same shape as the Kafka plugin's, so the workbench reads
/// one envelope.
/// </summary>
internal static class SurgewaveRecordHeaders
{
    /// <summary>Metadata the plugin reads itself; it addresses the record and is not a header.</summary>
    private static readonly HashSet<string> s_consumedByProduce = new(StringComparer.Ordinal) { "key", "partition" };

    /// <summary>
    /// The headers for a produce: every metadata entry that is neither a
    /// Bowire marker (<c>__bowire*</c>, which can hold secrets) nor one the
    /// plugin consumes. <c>null</c> when there are none.
    /// </summary>
    public static IReadOnlyDictionary<string, byte[]>? FromMetadata(IReadOnlyDictionary<string, string>? metadata)
    {
        if (metadata is null) return null;
        Dictionary<string, byte[]>? headers = null;
        foreach (var (name, value) in BowireMetadataKeys.WireHeaders(metadata))
        {
            if (s_consumedByProduce.Contains(name)) continue;
            headers ??= new Dictionary<string, byte[]>(StringComparer.Ordinal);
            headers[name] = Encoding.UTF8.GetBytes(value ?? "");
        }
        return headers;
    }

    /// <summary>
    /// The headers of a consumed record as <c>{ key, value, valueBase64 }</c>:
    /// <c>value</c> is the UTF-8 text, or <c>null</c> for bytes that are not;
    /// <c>valueBase64</c> always holds the bytes. <c>null</c> when there are none.
    /// </summary>
    public static IReadOnlyList<object>? ToEnvelope(IReadOnlyDictionary<string, byte[]>? headers)
    {
        if (headers is null || headers.Count == 0) return null;
        return [.. headers.Select(h => (object)new
        {
            key = h.Key,
            value = BowireSurgewaveProtocol.TryDecodeUtf8(h.Value ?? []),
            valueBase64 = Convert.ToBase64String(h.Value ?? []),
        })];
    }
}
