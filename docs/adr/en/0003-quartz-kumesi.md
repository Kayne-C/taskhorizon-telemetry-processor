# ADR 0003 — A Quartz.NET cluster and idempotent jobs

**Status:** Accepted

- Scheduler: Quartz.NET with a PostgreSQL job store (`qrtz_*` tables) and clustering enabled. In each cluster round a trigger fires on one node; if a node dies another takes over
  (test: ~6 runs in 6 s with two nodes, and the remaining node carries on once one is stopped).
- A cluster **reduces double execution, it does not guarantee against it** (for example, one round can run twice during a takeover). So every job is idempotent:
  - Rollup: it recomputes the range from scratch and writes with `ON CONFLICT DO UPDATE`. An incremental sum would be corrupted by a double run.
  - Alert: `UNIQUE (device, metric, bucket, kind)` + `DO NOTHING`.
  - Maintenance: `CREATE ... IF NOT EXISTS`, `DROP ... IF EXISTS`.
- Jobs are `[DisallowConcurrentExecution]`: the same job does not overlap itself in the cluster.
- The rollup recomputes only a bounded look-back window (`RollupLookbackHours`): the cost is bounded, and late data inside the window corrects itself.
  Late data outside the window has to be backfilled by hand (documented as a limit).
- The scheduler cluster and ingestion are independent: one node can run only the jobs and another only ingestion.
