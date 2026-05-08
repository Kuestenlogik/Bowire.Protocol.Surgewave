# Surgewave coverage

What this plugin covers from the Surgewave SDK surface, and what it deliberately doesn't (yet).

## Wire surface — what shows up in the Bowire stream

| Source | Carrier | Tested | Notes |
|--------|---------|:-:|-------|
| **Native Surgewave protocol** | `Kuestenlogik.Surgewave.Client.ISurgewaveClient` produce + consume APIs | ✅ | Default mode for `surgewave://` URLs. Native framing, partitioned consume, consumer-group state. |
| **Kafka-compat wire** | Same broker via Confluent.Kafka client when URL has `?protocol=kafka` | ✅ | The Surgewave broker speaks both wires; the plugin lets users pick which to inspect. |
| **Confluent Schema Registry decode** | `Confluent.SchemaRegistry` + `Confluent.SchemaRegistry.Serdes.Avro` | ✅ | When `?schema-registry=…` is on the URL, Avro / JSON / Protobuf payloads decode inline; envelope keeps the JSON projection plus an `encoding` tag. |
| **In-process tap** (`surgewave://embedded`) | `ISurgewaveBrokerObservability` event stream | ✅ | When Bowire is hosted inside the broker process, taps every `SurgewaveBrokerEvent` (Produced / Consumed / Rejected / Rebalanced) without going through the network. |
| **mTLS + SASL auth** | `__bowireMtls__` / `__bowireKafkaSasl__` metadata markers | ✅ | Same auth-helper markers as the Kafka plugin; markers stripped from metadata before the wire. |

## Plugin contract — `IBowireProtocol`

| Method | Behaviour | Tested |
|--------|-----------|:-:|
| `DiscoverAsync` | Connects, surfaces a `Cluster` service with broker metadata. Topic enumeration on the native protocol is pending an admin-API on the SDK. | ✅ |
| `InvokeAsync` | One-shot produce on `produce`. Status mirrors the broker's ack. | ✅ |
| `InvokeStreamAsync` | Server-streaming consume on `consume` (live), or in-process tap on `surgewave://embedded`. | ✅ |
| `OpenChannelAsync` | Returns `null` — Surgewave doesn't have duplex-channel semantics. | ✅ |

## Mock-emitter contract — `IBowireMockEmitter`

| Behaviour | Tested |
|-----------|:-:|
| Recordings tagged `protocol: "surgewave"` get re-published at the recorded cadence. | ✅ |
| First-step metadata (`bootstrap` / `bootstrap-servers`) overrides the default broker CSV. | ✅ |
| Per-step `key` / `partition` honoured on produce. | ✅ |
| `responseBinary` (base64) wins over text body for binary payloads — same precedence as the Kafka emitter. | ✅ |

## Coverage measurement

Run from the repo root:

```bash
dotnet test --collect:"XPlat Code Coverage" --results-directory artifacts/cov
```

Latest snapshot (xunit + Surgewave.TestKit, 54 tests):

| Component | Line | Branch |
|-----------|:----:|:------:|
| `SurgewaveConnection` | 100 % | 90 % |
| `SurgewaveConnection/Endpoint` | 100 % | 100 % |
| `SurgewaveMockEmitter` | 96 % | 100 % |
| `BowireSurgewaveProtocol` (sync) | 80 % | 45 % |
| Async state machines (Discover / Invoke / Stream) | 19–28 % | 17–37 % |
| **Package total** | **59 %** | **52 %** |

The async-state-machine gap is the connect-required path — exercising it end-to-end needs a running Surgewave broker fixture (or a substantial on-the-fly mock). Lifting these numbers ships in a follow-on slice once the testkit hooks land.

The Schema Registry decoder is duplicated from `Bowire.Protocol.Kafka` (`KafkaSchemaRegistry` + `AvroValueToJson`) by design — two consumers don't yet justify a third NuGet package; if a third Kafka-wire plugin shows up the natural refactor is to lift them into a shared library.
