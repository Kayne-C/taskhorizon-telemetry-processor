# ADR 0001 — Exactly-once effect: the checkpoint and the aggregates in one transaction

**Status:** Accepted

## Context

We accumulate aggregates by adding (`cnt = cnt + x`). Applying the same batch twice silently corrupts the numbers; never applying it loses events.
A process can die midway, and two nodes can head for the same partition.

## Decision

"Delivering a message exactly once" is impossible; **applying the effect exactly once** is possible. The source is replayable (partition + offset), so:

1. A batch is read from `partition_checkpoints.next_offset`.
2. One database transaction: `UPDATE partition_checkpoints SET next_offset = new WHERE partition = p AND next_offset = expected`.
   If the number of affected rows is not 1, a `FencingException` is thrown (another writer advanced it) and nothing is written.
3. In the same transaction the aggregates are upserted. Commit.

Crash points and outcomes: before commit (the transaction rolls back, the batch is read again); after commit but before the acknowledgement (the checkpoint has advanced, the batch is not applied again).
The test crashes randomly at three points with 15% probability each and compares the aggregates with an independent computation.

Ownership: a PostgreSQL advisory lock per partition (for efficiency, so that two nodes do not do the same work for nothing). **Correctness does not depend on the lock**, it depends on the CAS:
a zombie writer that lost its lock is also rejected by the checkpoint comparison.

## Consequences

- Every batch pays one full round trip (measured: ~165 ms per 50k-event batch); that is why batches are large and writes are pre-aggregated.
- The source must be replayable. Otherwise this pattern cannot be applied (see the README limits).
