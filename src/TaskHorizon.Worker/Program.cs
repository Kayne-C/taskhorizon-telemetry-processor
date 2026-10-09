using System.Diagnostics;
using TaskHorizon.Worker;
using TaskHorizon.Worker.Ingest;

var bench = args.Contains("--bench");
var app = TaskHorizonHost.Build(args.Where(a => a != "--bench").ToArray());

if (!bench)
{
    await app.RunAsync();
    return 0;
}

await app.StartAsync();
var engine = app.Services.GetRequiredService<IngestEngine>();
var stats = app.Services.GetRequiredService<IngestStats>();
var started = Stopwatch.StartNew();
await engine.Completion;
started.Stop();
Console.WriteLine($"events={stats.Events} batches={stats.Batches} restarts={stats.Restarts}");
Console.WriteLine($"wall={started.Elapsed.TotalSeconds:0.0}s active={stats.ActiveSeconds:0.0}s throughput={stats.EventsPerSecond:0} events/s avg_commit={stats.AverageCommitMs:0.0} ms");
await app.StopAsync();
return 0;
