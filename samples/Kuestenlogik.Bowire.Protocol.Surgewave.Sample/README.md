# Kuestenlogik.Bowire.Protocol.Surgewave.Sample

A **self-contained** Surgewave sample — no docker. Surgewave ships a
pure-.NET in-process broker (`Kuestenlogik.Surgewave.Hosting`), so this one
project runs the broker, a producer **and** the embedded workbench, and
the plugin reaches the broker through its in-process `surgewave://embedded`
tap:

- **Embedded** — `builder.AddSurgewave()` runs the broker in-process
  (in-memory storage, auto-create topics), and the workbench is mounted at
  `/bowire` with `surgewave://embedded` already in the Sources rail. Open
  the Surgewave source, `produce` a message to `bowire.sample`, then
  `consume` it — no external process needed.
- **Separate** — point the plugin at a real broker instead, e.g. one
  started from the [Surgewave](https://github.com/Kuestenlogik/Surgewave)
  repo (`dotnet run --project src/Kuestenlogik.Surgewave.Cli -- broker
  --port 9092`), then `bowire --url surgewave://localhost:9092`.

## Run

```pwsh
dotnet run --project samples/Kuestenlogik.Bowire.Protocol.Surgewave.Sample
```

- Embedded workbench: <http://localhost:5196/bowire> — the embedded broker
  is already in the Sources rail. Open the Surgewave source, `produce` a
  message to `bowire.sample`, then `consume` it.
- As a separate target against your own broker:

  ```pwsh
  bowire --url surgewave://localhost:9092
  ```

## Notes

- The plugin's native-wire discovery currently surfaces the `Cluster`
  service (broker metadata); topic enumeration is pending an SDK admin
  API, so type the topic name (`bowire.sample`) into the workbench's method
  dropdown for now.
- Surgewave's broker also speaks the Kafka wire — add `?protocol=kafka` to
  the URL to drive it that way against the same broker.
