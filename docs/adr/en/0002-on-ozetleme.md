# ADR 0002 — Pre-aggregation and mergeable statistics only

**Status:** Accepted

- Instead of writing raw events to the database we write `count, sum, min, max, sumsq` per `(device, metric, minute)`. This five-tuple is **mergeable**:
  the merge of two summaries equals the summary of the union of the events (verified in a test with random splits). Order and batch boundaries do not affect the result, which
  simplifies late or out-of-order events and replays.
- Mean = sum / count, variance = sumsq / count − mean². Percentiles are deliberately absent because they cannot be merged.
- Values are integer centi-units: sums are exact with no floating-point error, which makes the "exactly equal to the independent computation" test possible.
  `sumsq` stays within the `bigint` range for the value range and event counts used (value ≤ 100.00 → 10⁸ × event count).
- Merging the same key inside a batch cuts the number of writes about 29x (40M events → 1.37M rows). How large the gain is depends on event density.
- Rows are ordered by primary key before the upsert; different partitions already write disjoint sets of devices.
- The minute bucket is computed from event time; a write goes to the partition of the day the bucket falls in (partitions are created on demand).
