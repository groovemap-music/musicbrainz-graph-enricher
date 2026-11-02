# Spike harness: MusicBrainz tags onto the Discogs genre/style tree

Throwaway code for bead `gm-musicbrainz-graph-enricher-569`. The write-up is
[docs/tag-mapping-spike.md](../../docs/tag-mapping-spike.md). Nothing here is imported by
`brainzgraphinator`, and the wheel ships only that package. No production path calls a model.

## Inputs (public, not committed in full)

| Input | Source | Kept here |
| --- | --- | --- |
| MusicBrainz tag tables | `mbdump-derived.tar.bz2`, fullexport `20261101-125824` (data.metabrainz.org, sha256 `7547e470…5dc63` checked against `SHA256SUMS`) | `data/vocab_stats.json` (aggregates only) |
| MusicBrainz genre list | `https://musicbrainz.org/ws/2/genre/all?fmt=txt`, 2026-11-02 (2,220 names) | `data/mb_genres_20261102.txt` |
| Discogs masters | `discogs_20261101_masters.xml.gz` via design's shared fetch lock (`~/.cache/groovemap-spikes/dumps/`) | `data/discogs_tree_20261101.json`, `data/discogs_pairs_20261101.json` |

## Files

| File | Role |
| --- | --- |
| `vocab.py` | Q1/Q2: distinct tags, uses, top-N coverage, genre-list buckets |
| `discogs_taxonomy.py` | Q7: observed vs single-genre (`graph.part_of`) pairs, and the tree at genre share >= 0.20 |
| `data/sample.json` | 200 tags, seed 569: 90 from rank 1-1000, 70 from rank > 1000 with >= 20 uses, 40 with 5-19 uses |
| `data/labels.tsv` | The answer key, one row per tag: confidence, tag, uses, stratum, accepted answers, rule (R1-R6, defined in the file header), reviewer. **Agent-drafted (claude-opus-5-5), pending human review**; the 54 `unsure` rows come first |
| `common.py` | Tree, label expansion (rejects anything not in the tree), scoring |
| `string_match.py` | Deterministic baselines (exact, normalized) |
| `jev_beam.py` | TypeSafe Choice per node, greedy and beam K=3, cached in `data/captures/jev_cache.json` |
| `flat_claude.py` | One Claude request per tag over the flattened list, cached in `data/captures/claude_cache.json` |
| `evaluate.py` | Accuracy, separation sweep, cost and latency, into `results/summary.json` |

## Reproduce

```sh
cd spikes/tag-mapping
# Q1/Q2 and Q7 need the dumps above (paths are arguments):
python3 vocab.py <mbdump-dir> <mb_genres.txt> <out-dir>
gzip -dc discogs_20261101_masters.xml.gz | python3 discogs_taxonomy.py <out.json>
# Methods. With the committed caches these make no calls; --live allows paid calls on misses.
python3 string_match.py
uv run --no-project --with typesafe-sdk python jev_beam.py           # needs TYPESAFE_API_KEY with --live
uv run --no-project --with anthropic python flat_claude.py           # needs Anthropic credentials with --live
python3 evaluate.py
```
