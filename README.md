# Bowire.Protocol.Surgewave

[![CI](https://img.shields.io/github/actions/workflow/status/Kuestenlogik/Bowire.Protocol.Surgewave/ci.yml?branch=main&label=CI)](https://github.com/Kuestenlogik/Bowire.Protocol.Surgewave/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/Kuestenlogik/Bowire.Protocol.Surgewave/branch/main/graph/badge.svg)](https://codecov.io/gh/Kuestenlogik/Bowire.Protocol.Surgewave)
[![NuGet](https://img.shields.io/nuget/v/Kuestenlogik.Bowire.Protocol.Surgewave)](https://www.nuget.org/packages/Kuestenlogik.Bowire.Protocol.Surgewave)
[![License](https://img.shields.io/github/license/Kuestenlogik/Bowire.Protocol.Surgewave)](https://github.com/Kuestenlogik/Bowire.Protocol.Surgewave/blob/main/LICENSE)

Native [Kuestenlogik.Surgewave](https://github.com/Kuestenlogik/Surgewave) protocol plugin for the [Bowire](https://github.com/Kuestenlogik/Bowire) workbench. Browse topics, produce / consume messages, and replay recordings over the Surgewave wire protocol via the `Kuestenlogik.Surgewave.Client` SDK. Sibling to [`Bowire.Protocol.Kafka`](https://github.com/Kuestenlogik/Bowire.Protocol.Kafka) — Kafka plugin for generic Confluent.Kafka-based clusters, Surgewave plugin for Surgewave-native deployments.

## URL shapes

```
surgewave://broker.example:9092                                            # single broker, auto-detect protocol
surgewave://b1:9092,b2:9092                                                # bootstrap servers CSV
broker.example:9094                                                    # bare host:port also accepted
surgewave://broker:9092?protocol=surgewave                                     # force the native Surgewave wire
surgewave://broker:9092?protocol=kafka                                     # force the Kafka-compatible wire
surgewave://broker:9092?protocol=kafka&schema-registry=http://sr:8081      # Kafka-mode + Confluent Schema Registry decode
```

## Protocol mode

Surgewave's broker speaks both the native Surgewave wire and the Kafka wire — you can pick which one this plugin uses against the same broker. Defaults to **auto-detect** (try native first, fall back to Kafka), driven by the `?protocol=` query parameter:

| Value | Effect |
|-------|--------|
| `surgewave` / `native` / `surgewave-native` | `SurgewaveClientBuilder.UseSurgewaveProtocol()` — max performance, advanced features (SharedMemory transport, batching presets, handler dispatch) |
| `kafka` | `SurgewaveClientBuilder.UseKafkaProtocol()` — interoperable wire, works against generic Kafka tooling, Confluent Schema Registry support |
| `auto` (default when omitted) | `SurgewaveClientBuilder.UseAutoDetect()` |

Unknown values fall back to `auto` so a typo doesn't break discovery.

## Schema Registry (Kafka mode)

When the URL carries `?schema-registry=…` and the consumed message body is framed in the Confluent wire format (`0x00` magic byte + 4-byte big-endian schema id + Avro/JSON body), the plugin decodes it on the fly and the consume envelope's `value` field carries the JSON projection. An additional `encoding` field tags the schema kind (`"avro"`, …). Schemaless topics, plain UTF-8 / opaque binary stay in the original fallback path.

The wire-format decoder is duplicated from [`Bowire.Protocol.Kafka`](https://github.com/Kuestenlogik/Bowire.Protocol.Kafka) (same `KafkaSchemaRegistry` + `AvroValueToJson` types). Two consumers don't justify a third NuGet package — if a third Kafka-wire plugin shows up, the natural refactor is to lift them into a shared library.

## Discovery

Connects via `SurgewaveClient.Create(...).BuildAsync()` and surfaces a `Cluster` service for broker metadata. Topic enumeration on the native Surgewave protocol is pending an admin/metadata API on the client SDK — once that lands, topics populate the sidebar the same way the Kafka plugin does via `IAdminClient.GetMetadata`. Until then the plugin works by typing the topic name into the workbench's method dropdown.

## Consume / produce methods

Per topic, identical shape to the Kafka plugin:

| Method | Kind | Description |
|--------|------|-------------|
| `consume` | ServerStreaming | Subscribe + yield one JSON envelope per message (topic, partition, offset, timestamp, key/keyBase64, value/valueBase64, bytes) |
| `produce` | Unary | Publish one message; optional `key` and `partition` via metadata |

The envelope shape matches [`Bowire.Protocol.Kafka`](https://github.com/Kuestenlogik/Bowire.Protocol.Kafka) byte-for-byte so a recording captured against one plugin replays against the other (swap the step's `protocol` string).

## Mock replay

`SurgewaveMockEmitter` plugs into Bowire's mock server via `IBowireMockEmitter`. Recordings tagged `protocol: "surgewave"` get re-published at the original cadence. Metadata keys: `bootstrap` / `bootstrap-servers` for broker CSV (default `localhost:9092`), per-step `key` and `partition`. Payload source follows the common convention across all Bowire plugins: `responseBinary` (base64) wins, `body` (UTF-8) fallback.

## Local development

This plugin consumes `Kuestenlogik.Surgewave.Client` and `Kuestenlogik.Bowire` from sibling repos during local work:

```bash
# One-time setup
cd ../Bowire && dotnet pack -c Release
cd ../Surgewave   && dotnet pack -c Release

cd ../Bowire.Protocol.Surgewave
dotnet restore
dotnet build
dotnet test
```

The `nuget.config` in this repo points `bowire-local` at `../Bowire/artifacts/packages` and `surgewave-local` at `../Surgewave/artifacts/packages`; release builds resolve the same ids from nuget.org.

## Install (end-user)

```
dotnet tool install -g Kuestenlogik.Bowire.Tool
bowire plugin install Kuestenlogik.Bowire.Protocol.Surgewave
```

Then start the workbench and enter a `surgewave://` URL in the sidebar.
