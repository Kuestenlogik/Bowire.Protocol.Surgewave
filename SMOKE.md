# End-to-end smoke test

Verifies the whole chain: **pack Bowire + Surgewave SDK → install plugin → run broker + consumer → see messages flow into the Bowire workbench**. Takes about 90 seconds the first time (most of it NuGet restore), <15 s on repeat runs.

## Prerequisites

- Windows / Linux / macOS with the .NET SDK matching [`global.json`](global.json).
- Bowire main repo checked out as a sibling directory (`../Bowire`).
- Surgewave SDK checked out as a sibling directory (`../Surgewave`) — packs the `Kuestenlogik.Surgewave.Client` NuGet that this plugin links against.
- `bowire` CLI installed as a global tool — from the main repo:
  ```bash
  dotnet tool install --global --add-source ./artifacts/packages bowire
  ```

## Steps

### 1. Pack Bowire core + Surgewave SDK + this plugin

```bash
# Surgewave SDK (the Kuestenlogik.Surgewave.Client + Core packages)
cd ../Surgewave && dotnet pack -c Release

# Bowire core
cd ../Bowire && dotnet pack -c Release

# This plugin
cd ../Bowire.Protocol.Surgewave && dotnet pack -c Release
```

Each `dotnet pack` invocation lands in its own `artifacts/pkg/` (Surgewave) or `artifacts/packages/` (Bowire family).

### 2. Install the plugin

```bash
bowire plugin install Kuestenlogik.Bowire.Protocol.Surgewave \
    --source ../Bowire/artifacts/packages \
    --source ../Surgewave/artifacts/pkg \
    --source ./artifacts/packages
bowire plugin list
```

`plugin list` should show `Kuestenlogik.Bowire.Protocol.Surgewave` with the version that just got packed.

### 3a. Native-protocol smoke (in-process tap)

The simplest way to see the plugin work without standing up a broker process: run a Surgewave-hosted ASP.NET app that consumes its own broker via the in-process tap URL `surgewave://embedded`.

```csharp
using Kuestenlogik.Bowire;
using Kuestenlogik.Surgewave.Broker;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSurgewaveBroker();
builder.Services.AddBowire();

var app = builder.Build();
app.MapBowire();
app.Run();
```

Pre-seed the workbench with `surgewave://embedded` as a server URL, click `consume → orders` (any topic the broker has), and produce a message from a separate process — the Bowire stream surfaces it within milliseconds.

### 3b. Kafka-wire smoke

Surgewave's broker also speaks the Kafka wire. This is the path most Kafka-shop users take when they want to point existing tooling at Surgewave.

```bash
# In the Surgewave repo, start the broker on its default Kafka-compat port (9092).
dotnet run --project src/Kuestenlogik.Surgewave.Cli -- broker --port 9092

# In a second terminal: run Bowire against the broker via the Kafka wire.
bowire --url "surgewave://localhost:9092?protocol=kafka"
```

Browse to `http://localhost:5080/bowire`, pick the **Surgewave** protocol tab, and the topic list populates from the broker's metadata. Click `produce → my-topic` and send a record; click `consume → my-topic` and watch it land back via Confluent.Kafka's consumer.

### 4. Schema Registry smoke (Avro decode)

```bash
# Same broker as 3b, plus a Confluent Schema Registry instance on :8081.
bowire --url "surgewave://localhost:9092?protocol=kafka&schema-registry=http://localhost:8081"
```

Avro-encoded payloads decode inline — the streaming pane shows the JSON projection plus an `encoding: "avro"` tag on the envelope. Schemaless topics, plain UTF-8, and opaque binary keep the original fallback path.

### 5. Mock replay smoke

```bash
# Use a recording captured against either wire (the envelope shape is shared).
bowire mock --recording orders.bwr --port 7070
```

`SurgewaveMockEmitter` re-publishes every step tagged `protocol: "surgewave"` to the configured broker (CSV from the first step's `bootstrap` / `bootstrap-servers` metadata, default `localhost:9092`) at the recorded cadence.

### 6. Tear down

`Ctrl+C` in each running terminal.

```bash
bowire plugin uninstall Kuestenlogik.Bowire.Protocol.Surgewave
```

## What "passing" means

- The `bowire plugin install` step exits 0 and the package shows up in `plugin list`.
- `bowire --url surgewave://…` connects without auth / TLS errors.
- The Bowire workbench shows the **Surgewave** protocol tab.
- The streaming pane shows new frames every <1 second when a producer is active, with the topic / partition / offset surfaced on the envelope.
- Schema-Registry-tagged Avro payloads land as JSON in the pane (not raw bytes).
- The mock-emitter republishes a recorded session against a live broker without losing any step.

If any of those fails, the failure surface is one of: NuGet feed mis-configured (step 2), SDK / connection wiring (step 3), Schema Registry URL / auth (step 4), or the mock-emitter cadence (step 5). The unit tests in [`tests/`](tests/) fail-fast on the connection / mock-emitter problems; the smoke test catches the integration glue around them.
