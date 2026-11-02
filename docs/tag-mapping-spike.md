# Spike: MusicBrainz tags onto the Discogs genre/style tree

Bead `gm-musicbrainz-graph-enricher-569`. Status on 2026-11-02: **the key-free half is done;
the paid half is pending.** The vocabulary (Q1, Q2), the target tree (Q7), the labelled
sample and the string-match baseline are measured on real dumps. The Jev greedy and beam
K=3 runs, the flat-Claude baseline, the separation analysis (Q3 to Q6) and the GO / NO-GO
are **pending**. They wait on operator-named sources for `TYPESAFE_API_KEY` and
`ANTHROPIC_API_KEY`. The scripts are written and cached; each paid run is one command
(see [Pending runs](#pending-runs)).

The harness, data and reproduction steps are in
[spikes/tag-mapping/](../spikes/tag-mapping/README.md). It is throwaway code: nothing in
`brainzgraphinator` imports it, the wheel ships only `brainzgraphinator`, and no production
path calls a model. No projection or schema code was added.

The constraint stands: the retired roadmap
(`planning-archive/product-roadmap/ROADMAP.md`, lines 276-278) says an LLM is never the
source of catalog truth. Any adopted design classifies the distinct vocabulary offline into a
versioned, human-reviewed lookup, and ingest does an ordinary lookup.

The earlier version of this document was a dated prerequisite audit (2026-10-03). It is in git
history, and its stdlib tooling (`scripts/spike_tag_*.py`) is unchanged.

## Data used

All inputs are public dumps, read only. No database was written or queried.

- **MusicBrainz:** `mbdump-derived.tar.bz2` from fullexport `20261101-125824`, with its sha256
  checked against `SHA256SUMS`. From it, the `tag`, `artist_tag`, `label_tag`, `release_tag`
  and `release_group_tag` tables. These are the four entity types whose `tags`
  musicbrainz-ingestion keeps (`jsonl_parser.rs`). A *use* is one (entity, tag) row with a
  positive net vote count.
- **MusicBrainz genre list:** `/ws/2/genre/all`, 2,220 names, fetched 2026-11-02.
- **Discogs:** `discogs_20261101_masters.xml.gz`, fetched once under the design repo's shared
  fetch lock. It holds 2,608,936 masters. The releases file (10.6 GB) was not streamed.
  Masters are the sample for Q7, so pair counts are master-level, not release-level.

## Q1: vocabulary size and coverage

| Measure | Value |
| --- | --- |
| Distinct tags in the `tag` table | 246,340 |
| Distinct tags used on artists, labels, releases, release groups | **220,592** |
| Uses (entity-tag rows, net votes > 0) | **9,240,290** (release group 4.98M, release 3.48M, artist 0.75M, label 0.04M) |
| Used once / >= 5 uses / >= 20 uses | 137,590 / 31,081 / 9,880 tags |
| Share of uses covered by top 10 / 100 / 1,000 / 10,000 tags | 28.5% / **61.9%** / **86.5%** / 95.1% |
| Tags needed for 50% / 80% / 90% / 95% / 99% of uses | 48 / 432 / 1,887 / 9,551 / 128,190 |

The head is short and the tail is very long. About 2,000 tags carry 90% of uses, and the
last 4% of uses is spread over more than 100,000 tags.

## Q2: genre-like versus junk

There are two estimates.

- **Deterministic:** 1,887 of MusicBrainz's 2,220 curated genre names occur as tags, and they
  carry **76.9% of uses**. Year or decade patterns carry 0.2% and obvious personal or meta
  words 0.4%. The other 213,000 unlisted tags carry the remaining 22.5%. That share mixes
  genre-like long tail ("dungeon rap", "naturewave") with junk.
- **From the labelled sample (agent-drafted, see below):** the share of tags that are junk
  (NONE only) is **26% in the head** (rank 1-1,000; 6% of the head's uses), **50% in the mid
  band** (rank > 1,000 with >= 20 uses) and **70% in the tail** (5 to 19 uses). Junk is mostly
  places, artist and label names, personal tags, formats and moods. Fifteen tags read both
  ways ("piano", "vocal", "westcoast", "tv show").

So the use mass is mostly genre, and most distinct tags past the head are not.

## Q7: which Discogs tree

Two candidate pair sets come from the masters dump:

| Pair set | Pairs | Styles with 1 / 2 / 3 / 4 / 5 / 6+ parents |
| --- | --- | --- |
| Observed co-occurrence (any master) | 8,886 | 9 / 15 / 11 / 23 / 13 / **685** |
| `graph.part_of` rule (masters with exactly one genre) | 3,162 | 213 / 123 / 89 / 69 / 49 / **213** |
| **Tree used: genre share >= 0.20 of the style's masters** | **1,276** | **393 / 248 / 83 / 26 / 2 / 4** |

Neither raw set is a usable tree. Observed co-occurrence puts 685 of 756 styles under six or
more genres. The single-genre rule still gives 213 styles six or more parents, and 750 of its
pairs rest on one master. The spike uses the share rule that `insights.style_parents` already
applies (analytics-engine `insights/style_atlas.py`, parents at share >= 0.20, the rule the
Atlas spike measured). The dominant parent is always kept. Only 150 of the tree's pairs are
missing from `part_of`. One junk genre (`1`, 6 masters) is dropped. The result is 15 genres
and 756 styles, and each genre has at most 248 children, under TypeSafe's 255-option Choice
limit.

**Styles under several genres** are kept as separate full paths (`Electronic/Trip Hop`,
`Hip Hop/Trip Hop`). The root has a NONE option and every genre has a genre-only option.
Scoring accepts any tree parent of a style unless the tag itself names the genre. That
exposes the ambiguous genre step beam search is meant for. Building the tree from
co-occurrence is deterministic and needs no model. Fixing `part_of` for multi-genre records
stays out of scope here.

## Labelled sample

`data/sample.json` holds 200 distinct tags drawn with seed 569, stratified as 90 head, 70 mid
and 40 tail (populations 1,000, 8,880 and 21,201). Tags with fewer than 5 uses are excluded.
`data/labels.tsv` is the answer key, one row per tag with confidence, uses, stratum, accepted
answers, the rule applied (R1-R6, written in the file header) and a reviewer column. Of the
200 rows, 54 are `unsure` and come first, 50 are `medium` and 96 are `high`. The key holds
86 NONE-only tags, 99 genre or style tags and 15 that accept both.

**The labels are agent-drafted (claude-opus-5-5) and pending human review.** No human labels
existed. The team lead approved drafting them against written rules, and the operator will
review them before the paid runs. Two caveats follow:

- Every accuracy here is against an unreviewed key.
- The flat-Claude baseline will be graded by a key a Claude model wrote. Read its score as
  optimistic until a person has reviewed the key.

## Results: string matching (Q6, deterministic half)

| Method | Exact | Genre-like tags found | Junk -> NONE | Head / mid / tail | Precision when it assigns |
| --- | --- | --- | --- | --- | --- |
| Case-insensitive exact name | 66.5% | 32.3% | 100% | 63.3% / 62.9% / 80.0% | 100% (35 of 35) |
| Normalized (`&`/`n` -> `and`, punctuation and spaces removed) | 70.5% | 40.4% | 100% | 68.9% / 67.1% / 80.0% | 100% (43 of 43) |
| Jev greedy | *pending* | | | | |
| Jev beam K=3 | *pending* | | | | |
| Flat Claude (claude-opus-5-5) | *pending* | | | | |

A non-match predicts NONE, which is why junk scores 100%. The overall figures reward that
default, so the genre-like column is the fair comparison. Over the whole vocabulary,
normalized matching hits 1,774 tags carrying **69.9% of all uses**, and it covers 86.8% of the
uses of tags on MusicBrainz's genre list. In the sample it made no wrong assignment. Its
misses are compounds and variants ("melodic black metal", "punk rock", "progressive rock",
"conscious hip hop"), spelling outside the normalizer ("no-wave" works, "experiemental" does
not), translations ("シンセウェイブ") and genre-like tags with no Discogs style.

What this means for the design: an ordinary deterministic lookup already places most tag
uses. Any model's value is in the long tail of variants, which is exactly the part a person
must review.

## Pending: Q3 to Q6 with models

These sections are **pending** until the paid runs complete. `evaluate.py` fills
`results/summary.json` with every number below from the caches, with no further calls.

- **Q3, accuracy:** greedy versus beam K=3 on the 200 tags, overall, by stratum, genre-like
  versus junk, and genre-level versus exact.
- **Q4, separation:** AUROC of separation (and path score) for correct versus wrong. A sweep of
  thresholds 1.0 to 100 reports auto-accepted count and precision, review-queue size, errors
  caught, and the review queue extrapolated to the 31,081 tags with >= 5 uses.
- **Q5, cost and latency:** measured input tokens, requests per tag, per-request p50/p95
  latency and wall time per tag, extrapolated to tags with >= 20 uses, >= 5 uses and all
  distinct tags. Cache savings: greedy reuses beam's root and genre questions, so beam plus
  greedy costs beam's requests alone. A warm replay makes zero calls.
- **Q6, flat Claude:** one claude-opus-5-5 request per tag over all 1,292 options (NONE, 15
  genre-only, 1,276 leaves), with the list in a cached system prompt and JSON-schema output.

### Pre-run estimates (not measurements)

These are from prompt sizes: about 3.3k input tokens per tag at most for beam K=3, and a
10k-token cached system prompt for Claude.

| Run | Sample (200 tags) | Tags >= 5 uses (31,081) | All distinct (220,592) |
| --- | --- | --- | --- |
| Jev `jev-1.14.0`, beam K=3 plus greedy ($0.042 / Mtok input, output free) | <= $0.03 | <= $4.30 | <= $31 |
| Flat Claude `claude-opus-5-5` ($4 in, $0.20 cache read, $20 out per Mtok, effort medium) | about $2-6 | about $300-900 | not sensible |

Rate limits for TypeSafe are set per organization in its console. The SDK retries 429s and
honors `retry-after`. The measured figures will replace these estimates.

### Pending runs

Run from `spikes/tag-mapping/`. Each writes only its cache and its results file, then run
`python3 evaluate.py`.

```sh
TYPESAFE_API_KEY=... uv run --no-project --with typesafe-sdk python jev_beam.py --live
ANTHROPIC_API_KEY=... uv run --no-project --with anthropic python flat_claude.py --live
python3 evaluate.py
```

**Model pin.** The bead and the cookbook name `jev-1.12`, but
[docs.typesafe.ai/models](https://docs.typesafe.ai/models) now lists only `jev-1.14.0` and
`jev-1.13.0`. The harness pins the versioned `jev-1.14.0`, never an alias, and aborts if a
response reports another model.

## GO / NO-GO

**Withheld** until Q3 to Q6 are measured. The decision turns on two numbers:

- whether beam K=3 finds clearly more of the genre-like long tail than normalized string
  matching (40% here) at high precision;
- whether separation makes the review queue small enough for a person, in the low thousands
  for the >= 5-uses vocabulary.

### Provisional ADR outline (only if GO)

- **Lookup:** `design/taxonomy/mb-tag-genre/v1/`, holding `mb-tag-genre.json` (tag ->
  `NONE` | genre | genre/style, with method, source dump date, model id, separation and
  reviewer), a JSON Schema, and fixtures. This is the same pattern as `media/v1` and
  `company-roles/v1` (ADR 0007/0011).
- **Order of precedence:** normalized string match, then reviewed model proposals. Tags below
  the auto-accept threshold, and every model-only proposal in a first version, go to a person.
  Unreviewed rows never ship.
- **Versioning:** a new `vN` when the Discogs tree or the tag vocabulary drifts. The tree is
  frozen from a named dump with the share >= 0.20 rule.
- **Drift check:** a script that diffs the current dump's tags with >= N uses against the
  lookup, and lists new tags and tree changes as review work. It runs offline in CI, with no
  model calls.
- **TypeSafe dependency:** offline and batch only, from a pinned versioned model, with captures
  cached and committed so a rerun is reproducible. Ingest and the enricher never call it.

### Candidate follow-up beads (for a replan, if GO)

1. Human review of `data/labels.tsv`, then run and score the paid methods.
2. A deterministic Discogs tree export (share >= 0.20, releases plus masters) as a versioned
   design artifact.
3. A string-match-first lookup generator plus review workflow for `mb-tag-genre/v1`.
4. Offline Jev batch over the >= 5-uses residue, with a review queue at the chosen threshold.
5. An enricher projection of MusicBrainz tags through the frozen lookup (plain lookup, no model).
6. A drift-check script and a CI job.
