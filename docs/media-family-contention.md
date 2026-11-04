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

## Follow-up to file

Restore parallelism under contention by writing media in one global lock order: merge the
`MediaFamily` rows sorted by name, then the `Medium` rows sorted by id, then the edges, and delete
stale `ISSUED_ON` edges in `Medium` id order. Acceptance: the stress shape above shows zero
`DeadlockDetected` retries with parallel flushes, and the prune race test still passes. This
needs its own bead because it reshapes the media statements.

## Spike gm-musicbrainz-graph-enricher-569

Confirmed before adding any genre statement: `docs/tag-mapping-spike.md` records that spike as
**blocked, with no GO/NO-GO verdict** (no tag-bearing corpus, no labels, no measured cost). No genre
or style statements were added, and none should be until it produces a verdict.
