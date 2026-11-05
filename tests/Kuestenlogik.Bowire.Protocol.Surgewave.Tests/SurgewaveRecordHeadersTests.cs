// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using System.Net;
using System.Net.Sockets;
using Kuestenlogik.Surgewave.Hosting;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Bowire.Protocol.Surgewave.Tests;

/// <summary>
/// #86 — the metadata of a produce goes out as record headers, but for the
/// Bowire markers, which can hold secrets, and the <c>key</c> and
/// <c>partition</c> the plugin addresses the record with. A consumed record
/// shows the headers it carries.
/// </summary>
public sealed class SurgewaveRecordHeadersTests
{
    [Fact]
    public void Metadata_Becomes_Headers_Without_Markers_Key_Or_Partition()
    {
        var headers = SurgewaveRecordHeaders.FromMetadata(new Dictionary<string, string>
        {
            ["trace-id"] = "abc",
            ["key"] = "user-1",
            ["partition"] = "0",
            ["__bowireKafkaSasl__"] = "{\"password\":\"secret\"}",
        });

        var header = Assert.Single(headers!);
        Assert.Equal(("trace-id", "abc"), (header.Key, Encoding.UTF8.GetString(header.Value)));
    }

    [Fact]
    public void No_Header_Worth_Sending_Means_No_Headers()
    {
        Assert.Null(SurgewaveRecordHeaders.FromMetadata(null));
        Assert.Null(SurgewaveRecordHeaders.FromMetadata(new Dictionary<string, string> { ["key"] = "k" }));
    }

    [Fact]
    public void A_Consumed_Record_Lists_Its_Headers_With_Binary_As_Base64()
    {
        var json = JsonSerializer.Serialize(SurgewaveRecordHeaders.ToEnvelope(new Dictionary<string, byte[]>
        {
            ["trace-id"] = Encoding.UTF8.GetBytes("abc"),
            ["blob"] = [0xFF, 0xFE],
        }));
        using var doc = JsonDocument.Parse(json);
        var items = doc.RootElement.EnumerateArray().ToDictionary(i => i.GetProperty("key").GetString()!);

        Assert.Equal("abc", items["trace-id"].GetProperty("value").GetString());
        Assert.Equal(JsonValueKind.Null, items["blob"].GetProperty("value").ValueKind);
        Assert.Equal("//4=", items["blob"].GetProperty("valueBase64").GetString());
        Assert.Null(SurgewaveRecordHeaders.ToEnvelope(null));
    }
}

/// <summary>
/// #86 against a real broker: Surgewave's own, in process. The plugin
/// produces over the native protocol, and the record is read back with the
/// Confluent client over the broker's Kafka wire, at exactly the offset
/// the produce reported.
/// </summary>
public sealed class SurgewaveRecordHeadersE2ETests
{
    private static int FreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    [Fact]
    public async Task A_Produced_Header_Arrives_On_The_Record()
    {
        var ct = TestContext.Current.CancellationToken;
        // The broker serves the native and the Kafka wire on one port.
        var port = FreePort();
        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(o => o.Listen(IPAddress.Loopback, 0));
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Surgewave:Host"] = "127.0.0.1",
            ["Surgewave:Storage"] = "Memory",
            ["Surgewave:AutoCreateTopics"] = "true",
            ["Surgewave:Port"] = port.ToString(System.Globalization.CultureInfo.InvariantCulture),
        });
        builder.AddSurgewave();
        await using var broker = builder.Build();
        await broker.StartAsync(ct);

        const string topic = "bowire-surgewave-headers";
        var protocol = new BowireSurgewaveProtocol();
        var produced = await protocol.InvokeAsync(
            $"surgewave://127.0.0.1:{port}?protocol=surgewave", topic, BowireSurgewaveProtocol.ProduceMethodName,
            ["""{"id":1}"""], false,
            new Dictionary<string, string> { ["key"] = "k1", ["trace-id"] = "abc-123", ["__bowireMtls__"] = "secret" },
            ct);
        Assert.True(produced.Status == "OK", produced.Status);
        var offset = long.Parse(produced.Metadata["offset"], System.Globalization.CultureInfo.InvariantCulture);
        var partition = int.Parse(produced.Metadata["partition"], System.Globalization.CultureInfo.InvariantCulture);

        using var consumer = new ConsumerBuilder<byte[]?, byte[]>(new ConsumerConfig
        {
            BootstrapServers = $"127.0.0.1:{port}",
            GroupId = "bowire-surgewave-headers-" + Guid.NewGuid().ToString("N"),
            EnableAutoCommit = false,
        }).Build();
        consumer.Assign(new TopicPartitionOffset(topic, partition, offset));
        var record = consumer.Consume(TimeSpan.FromSeconds(20));

        Assert.NotNull(record);
        var header = Assert.Single(record!.Message.Headers);
        Assert.Equal("trace-id", header.Key);
        Assert.Equal("abc-123", Encoding.UTF8.GetString(header.GetValueBytes()));
    }
}
