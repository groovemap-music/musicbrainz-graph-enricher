# Cypher performance audit: MusicBrainz graph enricher (2026-11-04)

Bead: `gm-musicbrainz-graph-enricher-545`. Read-only audit. No production code changed.
Sibling method: `discogs-graph-enricher` docs (`query-performance-optimizations.md`,
`file-completion-tracking.md`).

## Scope and limits

- **Statements audited:** the seven write statements in `brainzgraphinator/_projections.py`
  (media summary, media edges, relationship MERGE, and the Artist, Label, Release and
  Master enrichment SETs), plus the `RETURN 1` health ping at
  `brainzgraphinator/brainzgraphinator.py:1169`. The bead's "about 13 lines" counts Cypher
  text lines; there are eight statement sites.
- **Not in this repo: SQL.** `musicbrainz-graph-enricher` has no Postgres access. It consumes
  RabbitMQ messages that `musicbrainz-sql-loader` (`brainztableinator/_persistence.py`) has
  already built. The partial-Postgres load therefore does not change any number here. The SQL
  timings the operator asked for are **not measured** in this report. See follow-up F7.
- **Graph size.** The live graph is about 33.8M nodes and 134.3M relationships
  (`deployment/docs/architecture.md`, lines 1073–1084). The disposable graph was 600,012
  nodes and 550,000 relationships. Use it for plan shape and per-statement hit counts, not for
  absolute time. Lux has not been touched.
- **Environment.** Neo4j `2026-community` (the digest pinned in `scripts/test-integration.sh`),
  run in Docker on macOS with `--rm`, no volume, 512 MB heap, 256 MB page cache. Bolt is
  reached over Docker's loopback port forward. Per-transaction latency here is inflated
  compared with lux. Compare ratios, not milliseconds.
- **Constraints match `database-schema`** (`src/groovemap_schema/neo4j.py`): unique
  constraints on `Artist.id`, `Label.id`, `Master.id`, `Release.id`, `Medium.id`, and
  `MediaFamily.name`. The first load ran without them, which made every id lookup a label scan.
  That run was discarded. Every number below is from the constrained rerun.

## Method

1. Read each statement and its call path, and confirm where transactions start and end.
2. Load the synthetic sample: 200k Artist, Release and Medium nodes, 12 MediaFamily nodes,
   200k `IN_FAMILY`, 200k `ISSUED_ON` (source `musicbrainz`), and 150k `MEMBER_OF`.
3. `PROFILE` each statement shape with representative parameters (one-item and three-item
   media rows, one-relation MERGE, an Artist SET with all ten properties).
4. Time per-record transactions against one `UNWIND` batch. This copies the enricher's
   `make_message_handler` shape, which runs one `session.execute_write` per message.
5. Check a suspected orphan: remove a release's media edges and count Medium nodes left with
   no `ISSUED_ON`.

## Measured evidence

### PROFILE (db hits, from the constrained sample)

| Statement | Plan | DB hits | Notes |
|---|---|---|---|
| Artist SET (`enrich_artist`, 10 props) | `NodeUniqueIndexSeek(Locking)` on `Artist(id)` | 21 | Index seek. 19 hits are property writes. |
| Relationship MERGE (`create_relationship_edges`) | two unique seeks, `LockingMerge`, `Expand(Into)` | 12 | `Expand(Into)` reads the source Artist's outgoing relationships of that type (8 hits at degree 1). |
| Media summary (`RELEASE_MEDIA_SUMMARY_CYPHER`) | seek on `Release`, expand `ISSUED_ON`, delete | 11 | `Eager` on `mb_media_families` and `mb_medium_count` (read/set conflict). |
| Media edges, 1 row | `MergeUniqueNode` on `Medium` and `MediaFamily`, `LockingMerge` | 21 | `Eager` on `family` (read/set conflict). |
| Media edges, 3 rows | same plan | 51 | Linear in rows. Memory for the `Eager` materialisation is about 6.5 KB at 3 rows. |

Every id lookup is an index seek. No statement scans a label. The entity SETs are
`MATCH`-then-`SET` with no `MERGE`, which is why no stub nodes are created (see F5).

### Per-record versus batched writes (Artist SET, 2,000 records)

| Run | Per-record (one transaction each) | One UNWIND batch | Ratio |
|---|---|---|---|
| 1 | 31.5 s (15.7 ms/record) | 0.26 s (0.13 ms/record) | 120× |
| 2 | 10.8 s (5.4 ms/record) | 0.18 s (0.09 ms/record) | 58× |

The enricher runs the per-record shape. `make_message_handler` calls
`session.execute_write(tx_fn)` once per message, and the log line at
`brainzgraphinator.py:856` says "a flush of batch size 1". The legacy `NEO4J_BATCH_SIZE`
and `NEO4J_BATCH_MODE` knobs (lines 137–138) are read but do not change this path. The
cost is per-transaction overhead, and it applies to every message regardless of its
statements.

### Orphan Medium check

Removing a release's only `ISSUED_ON` edge via the media summary statement left 1 Medium
node with no incoming `ISSUED_ON`. Medium count is 200,002 after the benchmark and the
PROFILE runs, so nothing removes Medium nodes after their last edge is gone.

## Ranked problems

**1. One transaction per message (high).** Each message is its own `execute_write` with a
batch of one (`brainzgraphinator.py:856–861`). Measured overhead is 5–16 ms per record here,
about 58–120× a batched write of the same statement. A message can run up to seven statements
inside that transaction (`enrich_release` runs the SET, the summary and the edges). The
Discogs enricher solved this with `UNWIND` batches over records
(`discogs-graph-enricher/docs/query-performance-optimizations.md`), so this is the largest
gap between the two enrichers. It does **not** carry over in the same form: the Discogs fix
is the template, not a shared code path.

**2. No idempotency gate; every replay rewrites (medium).** The Discogs rule "a matching
`sha256` skips an unchanged entity" does not exist here. All four entity SETs write
`mb_updated_at = datetime.now(UTC)` (`_projections.py:191, 230, 281, 315`), so every replay
changes the property and performs a write. Requeued messages and re-runs therefore cost a
full write each. The fix is to hash the MusicBrainz payload, skip when it matches, and take
`mb_updated_at` from the source timestamp instead of the wall clock.

**3. Orphan Medium nodes accumulate (medium).** The media summary deletes stale `ISSUED_ON`
edges (`_projections.py:30–37`) but never deletes the `Medium` node, which the measured case
confirms. Discogs' stub sweep removes only Artist, Label, Master and Release stubs
(`discogs-graph-enricher/docs/file-completion-tracking.md`, step 2), so these orphans stay
forever. This does **not** carry over as a stub problem, because the MB enricher creates no
stubs. It is a separate, Medium-specific leak.

**4. Lock contention on shared MediaFamily nodes (medium, not measured).** Each media row runs
`MERGE (f:MediaFamily {name})` then a `LockingMerge` that locks `(m, f)`
(`_projections.py:40–47`). MediaFamily is a small set, so concurrent flushes that touch the
same family serialise on it. The disposable run was single-threaded and shows no contention.
This needs a concurrency test before a number can be put on it.

**5. No stubs and no aggregates here (informational).** The enricher never `MERGE`s an
Artist, Label, Master or Release, so no Discogs-style stub sweep is needed, and it recomputes
no aggregates. The Discogs 120 s timeouts were in the genre/style aggregate sweeps. They do
not apply here unless spike `gm-musicbrainz-graph-enricher-569` adds genre-level statements.
That spike was not in the bh store, so its design could not be read.

**6. Per-relation round trips and `Expand(Into)` degree cost (low).**
`create_relationship_edges` issues one `tx.run` (and one `consume()`) per relation
(`_projections.py:146–160`). Within the transaction that is one round trip per relation. The
MERGE cost scales with the source Artist's out-degree for that relationship type, which the
sample could not stress (no hub artists). Batch by type with `UNWIND` when fixing F1.

**7. `Eager` in the media statements (low).** `Eager` materialises the whole `UNWIND`. It is
harmless one row at a time, but a batched rewrite will materialise a whole batch. Size batches
with that in mind.

## Scaling notes for the full graph

- **Unique-key seeks** (every entity SET, the relationship endpoints, `Medium` and
  `MediaFamily` lookups): constant hits per statement at any graph size. The sample results
  carry over.
- **`Expand(Into)` in the relationship MERGE:** cost is the source Artist's out-degree for
  that relationship type. Hub artists on lux will cost more than the sample shows.
- **Media summary:** cost is the release's `ISSUED_ON` degree, normally 1 to a few.
- **Per-transaction overhead (F1):** independent of graph size. Its ratio carries over;
  absolute milliseconds from this laptop do not.
- **Orphan Medium count (F3):** grows with media churn over time, so it is largest on a
  long-lived lux graph.
- **SQL timings:** not measured, as above. When measured, treat partial-Postgres numbers as a
  lower bound and scale by the row-count ratio (19.6M/3.97M releases; 10.3M/7.9M artists).

## Follow-up beads (proposed, not filed)

- **F1 (P1):** Batch MB enrichment with `UNWIND` across messages (per entity, bounded batch
  size), keeping one transaction per batch. Acceptance: the per-record vs batched benchmark
  ratio is reproduced on lux-equivalent hardware, and a flush covers N records. Keep the
  `_projections.py` statements; change how they are invoked.
- **F2 (P2):** Payload-hash idempotency gate on the MB entity SETs. Set `mb_updated_at` from
  the source timestamp. Acceptance: a replayed unchanged message performs no write.
- **F3 (P2):** Prune Medium nodes left with no `ISSUED_ON` and no `IN_FAMILY` referrer, scoped
  so Discogs-owned media is untouched. Acceptance: orphan count is zero after a media summary
  delete, and Discogs `ISSUED_ON` edges are unaffected.
- **F4 (P3):** Concurrency test for MediaFamily lock contention with N parallel flushes. Decide
  whether to change the family write pattern based on the result.
- **F5 (P3):** Confirm `gm-musicbrainz-graph-enricher-569` (tag mapping) design before any
  genre-level statement lands. Re-run this audit's PROFILE on those statements.
- **F6 (P3):** Batch relation MERGEs by type. Include a hub-artist fixture in the sample.
- **F7 (P2):** Audit the SQL in `musicbrainz-sql-loader` (`brainztableinator/_persistence.py`)
  when the Postgres load completes. Apply the lower-bound and row-count scaling rule above.

## Reproduction

Nothing here touched lux. The disposable container ran with `--rm`, no volume, and
loopback-only publishing, and it was removed after the measurements.

1. `docker run --rm` the pinned Neo4j digest from `scripts/test-integration.sh`, with
   `NEO4J_server_memory_*` capped as above.
2. Apply the six unique constraints and the `Artist.sha256` index from
   `database-schema` `src/groovemap_schema/neo4j.py` **before** loading. Verify with
   `SHOW CONSTRAINTS`.
3. Load the sample with `CALL {} IN TRANSACTIONS OF 10000 ROWS` (a single transaction
   exhausts the transaction memory pool).
4. `PROFILE` each statement in `_projections.py` with representative parameters.
5. Benchmark per-record transactions against one `UNWIND` batch, using the Neo4j Python driver
   from the worktree's locked environment.
