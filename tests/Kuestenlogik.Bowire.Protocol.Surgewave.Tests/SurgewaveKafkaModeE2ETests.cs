// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Kuestenlogik.Surgewave.Hosting;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Bowire.Protocol.Surgewave.Tests;

/// <summary>
/// #88 — <c>?protocol=kafka</c> speaks the Kafka wire. The plugin did not
/// ship the Kafka transport of the Surgewave client, so every call in that
/// mode failed with "requires the 'Kuestenlogik.Surgewave.Client.Kafka'
/// package", and the Kafka fallback of <c>auto</c> could not work either.
/// The in-process broker serves the native and the Kafka wire on one port;
/// here the plugin produces and consumes in Kafka mode, and the Confluent
/// client reads the record back as an independent Kafka-wire reader.
/// </summary>
public sealed class SurgewaveKafkaModeE2ETests
{
    private static int FreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    private static async Task<WebApplication> StartBrokerAsync(int port, CancellationToken ct)
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(o => o.Listen(IPAddress.Loopback, 0));
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Surgewave:Host"] = "127.0.0.1",
            ["Surgewave:Storage"] = "Memory",
            ["Surgewave:AutoCreateTopics"] = "true",
            ["Surgewave:Port"] = port.ToString(System.Globalization.CultureInfo.InvariantCulture),
            // A broker with memory storage still keeps its cluster id on disk,
            // by default under the working directory. Two brokers of one test
            // run would share that file, and the first start of both collided
            // on it.
            ["Surgewave:DataDirectory"] = Directory.CreateTempSubdirectory("bowire-surgewave-").FullName,
        });
        builder.AddSurgewave();
        var broker = builder.Build();
        await broker.StartAsync(ct);
        return broker;
    }

    [Fact]
    public async Task Kafka_Mode_Produces_And_Consumes()
    {
        var ct = TestContext.Current.CancellationToken;
        var port = FreePort();
        await using var broker = await StartBrokerAsync(port, ct);

        const string topic = "bowire-surgewave-kafka-mode";
        var url = $"surgewave://127.0.0.1:{port}?protocol=kafka";
        var protocol = new BowireSurgewaveProtocol();

        var produced = await protocol.InvokeAsync(
            url, topic, BowireSurgewaveProtocol.ProduceMethodName,
            ["""{"id":88}"""], false, new Dictionary<string, string> { ["key"] = "k88" }, ct);
        Assert.True(produced.Status == "OK", $"{produced.Status}: {produced.Response}");
        var offset = long.Parse(produced.Metadata["offset"], System.Globalization.CultureInfo.InvariantCulture);
        var partition = int.Parse(produced.Metadata["partition"], System.Globalization.CultureInfo.InvariantCulture);

        // An independent Kafka-wire reader sees the record where the produce
        // said it went.
        using var reader = new ConsumerBuilder<byte[]?, byte[]>(new ConsumerConfig
        {
            BootstrapServers = $"127.0.0.1:{port}",
            GroupId = "bowire-surgewave-kafka-mode-" + Guid.NewGuid().ToString("N"),
            EnableAutoCommit = false,
        }).Build();
        reader.Assign(new TopicPartitionOffset(topic, partition, offset));
        var record = reader.Consume(TimeSpan.FromSeconds(20));
        Assert.NotNull(record);
        Assert.Equal("""{"id":88}""", Encoding.UTF8.GetString(record!.Message.Value));

        // And the plugin's own consume, in Kafka mode, streams what is produced
        // while it runs. The stream is a live tail: it starts at the end of the
        // topic and has no signal for "subscribed", so the test produces until
        // the stream has yielded a record, and that is the signal it waits for.
        using var consumeCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var firstEnvelope = Task.Run(async () =>
        {
            await foreach (var envelope in protocol.InvokeStreamAsync(
                url, topic, BowireSurgewaveProtocol.ConsumeMethodName, [], false, null, consumeCts.Token))
            {
                return envelope;
            }
            return null;
        }, ct);
        while (!firstEnvelope.IsCompleted)
        {
            var again = await protocol.InvokeAsync(
                url, topic, BowireSurgewaveProtocol.ProduceMethodName,
                ["""{"id":88}"""], false, null, ct);
            Assert.True(again.Status == "OK", $"{again.Status}: {again.Response}");
            await Task.WhenAny(firstEnvelope, Task.Delay(TimeSpan.FromMilliseconds(200), ct));
        }
        var first = await firstEnvelope;
        await consumeCts.CancelAsync();
        Assert.NotNull(first);
        using var doc = JsonDocument.Parse(first!);
        Assert.Contains("88", doc.RootElement.GetRawText(), StringComparison.Ordinal);
    }
}
