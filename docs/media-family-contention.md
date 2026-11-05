# MediaFamily and Medium lock contention (measured 2026-11-04)

Bead `gm-musicbrainz-graph-enricher-0o4`. Follow-up to F4 and F6 of
`cypher-performance-audit-2026-11-04.md`.

## What was measured

`scripts/bench-contention.sh` starts a disposable Neo4j (the pinned `2026-community` digest,
loopback port, `--rm`, no volume) and delivers 3,000 releases through the real release handler.
Each release carries 1–3 media drawn from 36 `Medium` ids filed under 12 `MediaFamily` nodes, with
a skewed mix (a few formats dominate). The constrained schema matches `database-schema`. It reports
wall time and the driver's transaction retries by Neo4j error code.

Two shapes, each with parallel flushes (the default) and with
`NEO4J_SERIALIZE_FLUSHES=true`. A warm-up run is discarded and each pair runs twice because this
laptop's absolute times drift between runs; compare the retry counts and the ratios.

| Shape | Flushes | Time (s), two runs | Retries |
|---|---|---|---|
| prefetch 200, batch 100 (defaults) | parallel | 0.81, 0.94 | 0 |
| prefetch 200, batch 100 (defaults) | one at a time | 0.91, 1.0 | 0 |
| prefetch 1000, batch 20 | parallel | 4.11, 4.59 | 92, 100 `DeadlockDetected` |
| prefetch 1000, batch 20 | one at a time | 3.01, 2.08 | 0 |

The graph is identical in every run (4,815 `ISSUED_ON` edges, 12 families, no duplicate `Medium`).

## Findings

- **At the defaults there is no measurable contention.** A batch of 100 releases rarely overlaps
  another, and `MERGE` on the unique `Medium` and `MediaFamily` keys never created duplicates.
- **With many concurrent small flushes, releases deadlock.** Concurrent batches take locks on the
  same shared `Medium`/`MediaFamily` nodes in different orders. Neo4j detects each deadlock and the
  driver retries, so no delivery fails, but about 3% of writes were retried and throughput was
  1.5–2.4× lower than with one flush at a time.
- Only releases contend. Artists, labels and master releases touch no shared vocabulary node.
- `create_relationship_edges_batch` already issues one `UNWIND` per edge type (F6 is done by the
  batching bead). Its cost scales with the source Artist's out-degree for that type; the synthetic
  graph has no hub artists, so that part is still unmeasured.

## What changed

`NEO4J_SERIALIZE_FLUSHES` (default `false`) allows one committing batch per data type at a time.
The default is unchanged because the measurement shows no benefit, and a small cost, at the
default sizes. Turn it on if `BATCH_SIZE` is lowered or the prefetch raised and the logs show
`DeadlockDetected` retries.

## Lock order (gm-musicbrainz-graph-enricher-03x2)

Media writes now take their locks in one global order, in both the batch and the per-message path
(`lock_media_in_order` in `brainzgraphinator/_projections.py`): every `MediaFamily` the write
touches sorted by name (including the family of any medium it detaches, because deleting a pruned
medium's `IN_FAMILY` edge locks the family), then every `Medium` it attaches or detaches sorted by
id, then the edges. The stale `ISSUED_ON` delete runs in `Medium` id order. The set of writes is
unchanged; only the order in which locks are taken differs.

Same bench (`scripts/bench-contention.sh`, 3,000 releases, two runs per row, real Neo4j), stress
shape prefetch 1000 / batch 20 with parallel flushes:

| | Before | After |
|---|---|---|
| `DeadlockDetected` retries | 141, 95 | 0, 0 |
| Time (s) | 14.38, 8.17 | 7.95, 4.19 |

Defaults (prefetch 200 / batch 100) had 0 and 1 retries before and 0, 0 after. The graph is the same
in every run (4,815 `ISSUED_ON` edges, 12 families, no duplicate `Medium`). Absolute times drift
between runs on this laptop; the retry counts are the result. With the order fixed, parallel and
one-at-a-time flushes take about the same time at the stress shape, so `NEO4J_SERIALIZE_FLUSHES`
is no longer needed to avoid deadlocks; it stays as an opt-in.

`test_concurrent_batches_sharing_vocabulary_never_deadlock` (real Neo4j) runs 12 concurrent raw
transactions that rewrite and prune overlapping media. It fails with deadlocks on the previous
statements and passes now; the prune race test still passes.

## Spike gm-musicbrainz-graph-enricher-569

Confirmed before adding any genre statement: `docs/tag-mapping-spike.md` records that spike as
**blocked, with no GO/NO-GO verdict** (no tag-bearing corpus, no labels, no measured cost). No genre
or style statements were added, and none should be until it produces a verdict.
