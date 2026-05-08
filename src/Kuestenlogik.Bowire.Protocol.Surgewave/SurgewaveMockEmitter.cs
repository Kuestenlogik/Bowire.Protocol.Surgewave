// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Globalization;
using Kuestenlogik.Bowire.Mocking;
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Bowire.Protocol.Surgewave;

/// <summary>
/// <see cref="IBowireMockEmitter"/> implementation that replays
/// recorded Surgewave-protocol traffic. Same envelope / cadence contract
/// as the Kafka mock emitter — the only difference is the transport
/// (<see cref="SurgewaveClient"/> instead of Confluent.Kafka's producer).
/// Steps tagged <c>protocol: "surgewave"</c> are re-published to the
/// broker at the original cadence from
/// <see cref="BowireRecordingStep.CapturedAt"/>.
/// </summary>
public sealed class SurgewaveMockEmitter : IBowireMockEmitter
{
    private ISurgewaveClient? _client;
    private IProducer<byte[]?, byte[]>? _producer;
    private CancellationTokenSource? _cts;
    private Task? _schedulerTask;
    private bool _disposed;

    /// <inheritdoc />
    public string Id => "surgewave";

    /// <inheritdoc />
    public bool CanEmit(BowireRecording recording)
    {
        ArgumentNullException.ThrowIfNull(recording);
        return recording.Steps.Any(IsSurgewaveStep);
    }

    /// <inheritdoc />
    public async Task StartAsync(
        BowireRecording recording,
        MockEmitterOptions options,
        ILogger logger,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(recording);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        var surgewaveSteps = recording.Steps.Where(IsSurgewaveStep).ToList();
        if (surgewaveSteps.Count == 0) return;

        var bootstrap = ReadBootstrap(surgewaveSteps[0]);
        _client = await SurgewaveClient.Create(bootstrap).BuildAsync(ct);
        _producer = _client.CreateProducer<byte[]?, byte[]>();

        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        _schedulerTask = Task.Run(() => RunAsync(surgewaveSteps, options, logger, _cts.Token), _cts.Token);

        logger.LogInformation(
            "surgewave-emitter sending → {Bootstrap} (steps={Count})", bootstrap, surgewaveSteps.Count);
    }

    private static bool IsSurgewaveStep(BowireRecordingStep s) =>
        string.Equals(s.Protocol, "surgewave", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Read the bootstrap-servers CSV from the step's metadata. Same
    /// fallbacks as <c>KafkaMockEmitter</c> so recordings written by
    /// either plugin's capture path replay against the other.
    /// </summary>
    internal static string ReadBootstrap(BowireRecordingStep first)
    {
        if (first.Metadata is not null)
        {
            if (first.Metadata.TryGetValue("bootstrap", out var csv) && !string.IsNullOrWhiteSpace(csv))
                return csv;
            if (first.Metadata.TryGetValue("bootstrap-servers", out var csv2) && !string.IsNullOrWhiteSpace(csv2))
                return csv2;
        }
        return "localhost:9092";
    }

    private async Task RunAsync(
        List<BowireRecordingStep> steps,
        MockEmitterOptions options,
        ILogger logger,
        CancellationToken ct)
    {
        if (_producer is null) return;

        var baseCapturedAt = steps[0].CapturedAt;
        var speed = options.ReplaySpeed;

        do
        {
            var scheduleStartTicks = Environment.TickCount64;

            foreach (var step in steps)
            {
                ct.ThrowIfCancellationRequested();

                if (speed > 0)
                {
                    var targetOffsetMs = (long)((step.CapturedAt - baseCapturedAt) / speed);
                    var elapsed = Environment.TickCount64 - scheduleStartTicks;
                    var waitMs = targetOffsetMs - elapsed;
                    if (waitMs > 0)
                    {
                        try { await Task.Delay(TimeSpan.FromMilliseconds(waitMs), ct); }
                        catch (OperationCanceledException) { return; }
                    }
                }

                await EmitAsync(step, logger, ct);
            }
        }
        while (options.Loop && !ct.IsCancellationRequested);
    }

    private async Task EmitAsync(BowireRecordingStep step, ILogger logger, CancellationToken ct)
    {
        var payload = DecodePayload(step, logger);
        if (payload is null) return;
        if (string.IsNullOrEmpty(step.Service))
        {
            logger.LogWarning(
                "surgewave-emitter skipping step '{StepId}': step.service (topic) is empty.", step.Id);
            return;
        }

        var key = step.Metadata?.TryGetValue("key", out var k) == true
            ? System.Text.Encoding.UTF8.GetBytes(k)
            : null;

        int? partition = null;
        if (step.Metadata?.TryGetValue("partition", out var partitionStr) == true &&
            int.TryParse(partitionStr, NumberStyles.Integer, CultureInfo.InvariantCulture, out var p))
        {
            partition = p;
        }

        try
        {
            if (partition is int p2)
            {
                await _producer!.ProduceAsync(step.Service, p2, key, payload, ct);
            }
            else
            {
                await _producer!.ProduceAsync(step.Service, key, payload, ct);
            }
            logger.LogInformation(
                "surgewave-emit(step={StepId}, topic={Topic}, bytes={Bytes})",
                step.Id, step.Service, payload.Length);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex,
                "surgewave-emitter send failed for step '{StepId}' on topic '{Topic}'; scheduler continues.",
                step.Id, step.Service);
        }
    }

    /// <summary>
    /// Decode the step's payload to bytes. Same precedence as DIS /
    /// UDP / Kafka: <see cref="BowireRecordingStep.ResponseBinary"/>
    /// (base64) wins; <see cref="BowireRecordingStep.Body"/> is
    /// UTF-8-encoded as a fallback.
    /// </summary>
    internal static byte[]? DecodePayload(BowireRecordingStep step, ILogger logger)
    {
        if (!string.IsNullOrEmpty(step.ResponseBinary))
        {
            try
            {
                return Convert.FromBase64String(step.ResponseBinary);
            }
            catch (FormatException ex)
            {
                logger.LogWarning(
                    "surgewave-emitter skipping step '{StepId}': malformed base64 payload ({Message}).",
                    step.Id, ex.Message);
                return null;
            }
        }
        if (!string.IsNullOrEmpty(step.Body))
        {
            return System.Text.Encoding.UTF8.GetBytes(step.Body);
        }
        logger.LogWarning(
            "surgewave-emitter skipping step '{StepId}': neither responseBinary nor body present.", step.Id);
        return null;
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        if (_cts is not null)
        {
            try { await _cts.CancelAsync(); }
            catch (ObjectDisposedException) { /* already torn down */ }
        }
        if (_schedulerTask is not null)
        {
            try { await _schedulerTask; }
            catch (OperationCanceledException) { /* expected */ }
            catch { /* scheduler cleanup is best-effort */ }
        }
        if (_producer is not null) try { await _producer.DisposeAsync(); } catch { /* best-effort */ }
        if (_client is not null) try { await _client.DisposeAsync(); } catch { /* best-effort */ }
        _cts?.Dispose();
    }
}
