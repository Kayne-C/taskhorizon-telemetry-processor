# TaskHorizon — High-Volume Telemetry and Background Job Processor

**English** · [Türkçe](README.tr.md)

A Worker Service that processes millions of IoT/ERP telemetry events **exactly once**, aggregates them per minute and per hour in PostgreSQL, and runs scheduled jobs on a
**clustered Quartz.NET** scheduler backed by PostgreSQL. .NET 10.

The question at the centre of the design: if a worker dies midway, two nodes go for the same partition, or a commit is cut in half, are the aggregates still **exactly right**?
This repo tests that by recomputing the events through an independent path and comparing the result with the database.

## What it does

| Area | Approach | Evidence |
|---|---|---|
| **Asynchronous processing** | Producer/consumer split per partition: one task reads events and aggregates them in memory, a bounded channel (2 batches) feeds the committing task. The next batch is prepared while the database write is in flight | Measured: 8M events in 3.6 s |
| **Pre-aggregation** | Events merge in memory by `(device, metric, minute)`; the database receives summaries, not events (`count, sum, min, max, sumsq`) | 40M events → 1.37M rows (~29x fewer) |
| **Exactly-once effect** | Batch summaries and the partition **checkpoint in one transaction**: `UPDATE ... WHERE next_offset = @expected` (compare-and-set) + upsert + commit. After a crash it re-reads from the checkpoint | Test: random crashes before / during / after commit (15% each) → summaries **exactly equal** to the independent computation |
| **Multiple nodes** | Partition ownership through PostgreSQL advisory locks; when a node dies its locks are released and another takes over. The CAS keeps a stale writer (a zombie that lost its lock) out | Test: 2 nodes share 8 partitions, and `node A events + node B events = total` (no double counting); a node is stopped midway and the other finishes |
| **Fair sharing** | Each node announces itself with a membership lock; the live node count gives a share of `ceil(partitions / nodes)`. A node holding more than its share releases partitions at a batch boundary | Test: the first node takes all 8 partitions, when the second joins the first drops to 4 and the second starts working; the total event count is still exact |
| **Late / out-of-order events** | The minute bucket comes from event time; summaries are mergeable (sums, min/max), so order does not matter | Test: ±60 s jitter, summaries correct |
| **Scheduled jobs** | Quartz.NET with a PostgreSQL job store, **clustered**: hourly rollup, alert generation, maintenance. All idempotent | Test: 2 nodes, ~6 runs in 6 s (not 12); when one node dies the other carries on |
| **Rollup** | Minute → hour, recomputed with `ON CONFLICT DO UPDATE` (not incremental); running it again does not change the result | Test: hours equal the sum of their minutes; late data shows up in the next round |
| **Alerts** | A threshold breach is raised once through `UNIQUE(device, metric, bucket, kind)` | Test: even if the job runs 3 times, each spike produces one alert |
| **Partitioning and retention** | `agg_minute` is range-partitioned by day; partitions are created on demand (an advisory lock prevents races) and expired partitions are `DROP`ped (no row deletes) | Test: concurrent partition creation, retention |

## Measurements

AMD Ryzen 7 9800X3D (8 cores / 16 threads), Windows, PostgreSQL 17 **in Docker with default settings** (same machine), Release build,
8 partitions × 500 devices each, batch size 50,000, at most 4 concurrent commits:

| Data | Time | Throughput | Avg commit | Output |
|---|---|---|---|---|
| 8M events | 3.6 s | 2.19M events/s | 173 ms | 303k minute rows |
| 40M events | 16.7 s | 2.39M events/s | 163 ms | 1.37M rows, 472 MB (with indexes), total count exactly 40,000,000 |

On the same data: the hourly rollup reduces 1.37M rows to 48k hourly rows in **1.3 s**; the alert scan takes 117 ms.

**How to read these numbers:** the event generator is cheap and in-process (reading from a message queue over the network and parsing JSON/protobuf would lower this considerably);
PostgreSQL and the application share one machine and CPU; one database instance, one node. The figure means "in this design the database side is not the bottleneck and pre-aggregation works",
not "two million events per second in production".

## Quick start

```bash
docker compose up -d --build        # PostgreSQL + 2 worker nodes (same database, same Quartz cluster)
curl localhost:5401/stats           # progress, events/s, partition checkpoints
curl "localhost:5401/devices/3/minutes?metric=0&limit=5"
curl localhost:5401/alerts
```

A comparison run without Docker: `dotnet run --project src/TaskHorizon.Worker -c Release -- --bench` (needs a PostgreSQL connection, passed through the
`ConnectionStrings__Db` environment variable). When all events are processed it prints a throughput summary and exits.

Values are integer **centi-units** (45.00 → 4500). That is what keeps the sums and the sum of squares (for variance) exact, free of floating-point error.

## Architecture

```
Source (partition p, offset o) ── read+aggregate task ─▶ [channel, 2 batches] ─▶ commit task ─▶ PostgreSQL
   pure function: same (p,o) → same event                                          one transaction:
                                                                                  1) checkpoint CAS   (lock/fence)
                                                                                  2) agg_minute upsert (merging)
                                                                                  3) commit
Quartz (clustered) ─▶ RollupJob · AlertJob · MaintenanceJob   (all idempotent)
```

- A device always lives in the same partition (partitioning by key, as in Kafka). Two partitions therefore never update the same row: no lock contention, no deadlock.
  Rows are written in primary-key order.
- Decisions: [docs/adr/en](docs/adr/en).

## Tests

```bash
dotnet test --solution TaskHorizon.slnx     # needs Docker (real PostgreSQL 17 via Testcontainers)
```

26 tests: source determinism, summary merging (a property test with random splits), commit atomicity, fencing, crash injection,
two nodes / node death / a late-joining node, partitioning, rollup, alerts, maintenance, the Quartz cluster. Five consecutive full runs passed, and the late-joiner test alone passed 10 times in a row.

**A real bug the tests found:** when two nodes start at the same time (one already ingesting), the schema installer took locks on tables even with `CREATE ... IF NOT EXISTS`,
and **deadlocked** with the running writes. The fix: installation is guarded by a version table, no DDL runs when it is already installed,
and the first install happens on a single node under an advisory lock.

**A second real bug:** when fair sharing was first written, the live node count was queried concurrently from 8 partition tasks over one connection
(an Npgsql connection is not thread-safe). In about one run in six this caused thousands of restarts a second and stalls; the count now runs in a single background loop.

## Known limits (an honest list)

- The source is a **replayable synthetic log** (a pure function of partition + offset). The "exactly once" guarantee rests exactly on that: being able to read the same offset again.
  A real Kafka/MQTT/Event Hubs adapter was **not written**; the same model applies to a broker with offsets, but it was not tried.
- Percentiles (p95, p99) are not supported: they cannot be merged. Count, sum, min, max and sum of squares (so mean and standard deviation) are.
- The rollup recomputes only the last `RollupLookbackHours` hours; for older late data `RollupService.RunAllAsync` has to be called by hand (no endpoint).
- Retention is computed from the **timestamp of the newest data**, not the wall clock (because the synthetic data is in the past).
- The partition count cannot be changed after the first data: the device → partition mapping breaks and the "two partitions never write the same row" assumption collapses. There is no repartitioning tool.
- Fair sharing is a simple `ceil(P / nodes)` rule; no weighted nodes, capacity-aware placement or hot-partition balancing.
- Ownership uses advisory locks, which are **incompatible** with PgBouncer in transaction-pooling mode (they are session locks).
- The Quartz schema was taken from Quartz's official PostgreSQL script (same repository, version 4.4.0) and embedded; if Quartz is upgraded the script must be re-synced.
  Clustering was tested with two **in-process** nodes (same database), not on separate machines.
- Alerts are only written to a table; there is no notification channel (e-mail, webhook).
- The API has no authentication. There is no endless-stream mode: `EventsPerPartition` is finite.
- Throughput was measured on one machine under the constraints above; multi-node scaling throughput was not measured.
