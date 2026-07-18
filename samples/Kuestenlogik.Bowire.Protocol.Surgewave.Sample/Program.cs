// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

// Combined Surgewave sample for Bowire — fully self-contained, no docker.
// Surgewave ships a pure-.NET in-process broker (via
// Kuestenlogik.Surgewave.Hosting), so one project runs the broker AND the
// embedded workbench, and the plugin reaches the broker through its
// in-process `surgewave://embedded` tap:
//
//   * Embedded — builder.AddSurgewave() runs the broker in-process
//     (in-memory storage, auto-create topics); the workbench is mounted at
//     /bowire with `surgewave://embedded` seeded into the Sources rail.
//     Open the Surgewave source, `produce` a message to `bowire.sample`,
//     then `consume` it — no external process needed.
//   * Separate — point the plugin at a real broker instead, e.g. one
//     started from the Surgewave repo
//     (dotnet run --project src/Kuestenlogik.Surgewave.Cli -- broker
//     --port 9092), then `bowire --url surgewave://localhost:9092`.
//
// Run:
//   dotnet run --project samples/Kuestenlogik.Bowire.Protocol.Surgewave.Sample
//   → open http://localhost:5196/bowire

using Kuestenlogik.Bowire;
using Kuestenlogik.Bowire.Sources;
using Kuestenlogik.Surgewave.Hosting;

// Force the Surgewave plugin assembly to load before AddBowire's
// reflection scan runs — the Kuestenlogik.Bowire 2.2.x contract scans
// loaded assemblies, so without an explicit type reference the plugin DLL
// wouldn't be loaded in time for discovery.
_ = typeof(global::Kuestenlogik.Bowire.Protocol.Surgewave.BowireSurgewaveProtocol);

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls("http://localhost:5196");

// In-process Surgewave broker (config from the "Surgewave" section of
// appsettings.json: in-memory storage, auto-create topics). The plugin's
// `surgewave://embedded` tap consumes it directly.
builder.AddSurgewave();

builder.Services.AddBowire();
builder.Services.AddBowireCatalogue(builder.Configuration);

var app = builder.Build();
app.MapBowire("/bowire");
app.MapGet("/", () => Results.Redirect("/bowire"));
await app.RunAsync();
