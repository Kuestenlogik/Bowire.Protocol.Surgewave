# Bowire.Protocol.Surgewave — Roadmap

## Shipped

- **v0.1 — Initial client-based plugin**. `surgewave://host:port` URLs connect via `Kuestenlogik.Surgewave.Client.SurgewaveClient`. `consume` / `produce` methods per topic with the same JSON envelope shape as `Bowire.Protocol.Kafka` so recordings round-trip between the two by swapping the step's `protocol` string. `SurgewaveMockEmitter` re-publishes recorded traffic on the broker for mock-server replay.
- **Embedded broker-observability hook (plugin side)**. `surgewave://embedded` URLs route past the TCP client and resolve `Kuestenlogik.Surgewave.Core.Observability.ISurgewaveBrokerObservability` from the host DI container. Discovery surfaces a synthetic `BrokerTap` service whose `consume` method streams every `SurgewaveBrokerEvent` (Produced / Consumed / Rejected / Rebalanced) as a superset of the normal consume envelope — adds `event` kind, `principal`, `reason`, `consumers[]`. Pairs with the Surgewave-side `SurgewaveBrokerObservability` service, which now publishes all four event kinds: Produced + Rejected from `DataApiHandler.HandleProduceAsync`, Consumed from `HandleFetchAsync` (one event per non-empty partition fetch), and Rebalanced from `ConsumerGroupCoordinator.HandleSyncGroup` (one event per rebalance, carrying the group id in `Consumers`).
- **Schema-registry-aware payload decoding (Kafka-wire mode)**. When the URL carries `?schema-registry=…` and consumed messages are framed in the Confluent wire format (`0x00` magic byte + 4-byte big-endian schema id + body), `BowireSurgewaveProtocol` decodes Avro on the fly via the `KafkaSchemaRegistry` + `AvroValueToJson` helpers. The envelope's `value` field then carries the JSON projection plus an `encoding: "avro"` tag. JSON-Schema-registered bodies decode the same way but are returned as raw text without an `encoding` tag (v0 limitation — Protobuf payloads still hit the base64 fallback). Schemaless topics, plain UTF-8, and opaque binary keep the original fallback path.

## Planned

### Topic discovery via admin metadata

Currently the plugin surfaces a synthetic `Cluster` service but doesn't enumerate topics — the `Kuestenlogik.Surgewave.Client` SDK doesn't yet expose a `GetMetadata` equivalent to Confluent's `IAdminClient.GetMetadata`. Once that lands upstream, discovery populates topics the same way the Kafka plugin does.

### JSON-Schema and Protobuf decode

The Avro path through the Confluent wire framing is in. JSON Schema currently surfaces as raw text (no `encoding` tag); Protobuf still falls back to base64. A follow-up wires both into the same `KafkaSchemaRegistry` + projector pattern so all three Confluent-wire types decode consistently and tag the envelope with the right `encoding` value.
