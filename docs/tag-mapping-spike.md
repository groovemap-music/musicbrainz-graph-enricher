# Spike: MusicBrainz tags onto the Discogs genre/style tree

Bead `gm-musicbrainz-graph-enricher-569`. Status on 2026-11-10: **complete. Recommendation:
GO, with one change to the planned design.** Every question (Q1 to Q7) is measured. The
accuracy figures are graded against an answer key the operator reviewed in full.

- Jev beam K=3 finds 80.5% of genre-like tags exactly, against 35% for normalized string
  matching. Flat Claude finds 88.6%.
- **Jev's separation score is not a usable auto-accept signal** (AUROC 0.64). The design
  must not rest on it.
- **Agreement between Jev and Claude is a usable signal.** When both name the same genre
  or style, 39 of 40 sample answers are right.
- The adopted design: string match first, then two offline model proposals, then a person
  reviews every model-proposed genre. For tags with 20 or more uses (about 95% of tag
  uses), that is about 4,200 review rows and about $55 of model calls, once.

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
- **From the reviewed sample:** the share of tags that are junk (NONE only) is **21% in the
  head** (rank 1-1,000; 4% of the head's uses), **43% in the mid band** (rank > 1,000 with
  >= 20 uses) and **65% in the tail** (5 to 19 uses). Junk is mostly places, artist and label
  names, personal tags, formats and moods.

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
answers, the rule applied (R1-R6, written in the file header) and a reviewer column.

The key was drafted by claude-opus-5-5 against the written rules on 2026-11-02. **The
operator reviewed all 200 rows on 2026-11-10** (`confidence=reviewed` and a named reviewer
on every row). The reviewed key holds 75 NONE-only tags, 123 genre or style tags and 2 that
accept both. The draft had 86, 99 and 15. The review mostly settled the draft's ambiguous
rows, and it accepted a genre for some person and role tags that rule R5 had sent to NONE
("composer", "schubert", "terry riley", "singer").

Grading changed accordingly. Against the draft, flat Claude scored 97.5% exact and Jev beam
93.5%. Against the reviewed key they score 92.0% and 85.5%. The draft favoured the models,
Claude most, so the numbers below are the ones to use.

## Q3 and Q6: accuracy

`results/summary.json` holds every figure. Exact means the predicted path is one the key
accepts. Genre-like means tags whose key has no NONE answer (123 tags).

| Method | Exact | Genre-like exact | Junk -> NONE | Head / mid / tail | Use-weighted | Precision when it assigns |
| --- | --- | --- | --- | --- | --- | --- |
| String, case-insensitive exact name | 56.0% | 28.5% | 100% | 57.8% / 47.1% / 67.5% | 71.0% | 100% (35 of 35) |
| String, normalized | 60.0% | 35.0% | 100% | 63.3% / 51.4% / 67.5% | 76.5% | 100% (43 of 43) |
| Jev greedy | 83.0% | 76.4% | 94.7% | 84.4% / 80.0% / 85.0% | 94.2% | 89.5% (of 105) |
| **Jev beam K=3** | **85.5%** | **80.5%** | 94.7% | 85.6% / 84.3% / 87.5% | 94.6% | 90.8% (of 109) |
| Flat Claude (claude-opus-5-5, effort medium) | **92.0%** | **88.6%** | 97.3% | 93.3% / 88.6% / 95.0% | 97.7% | 96.5% (of 115) |

- **Beam versus greedy:** beam K=3 gets 5 more tags right (171 against 166). All of the gain
  is on genre-like tags, which is where the ambiguous genre step is. Genre-level accuracy
  rises from 85.0% to 87.5%.
- **Model versus string matching:** both models find more than twice as many genre-like tags
  as normalized string matching. String matching never assigns a wrong genre. It just
  misses compounds ("melodic black metal"), variants, typos, translations and tags with no
  Discogs style of the same name.
- **Jev versus Claude:** Claude is more accurate everywhere. It also assigns a genre to junk
  less often (2 junk tags got a genre, against 4 for Jev).
- **Errors that matter:** a wrong genre on a tag puts a release in the wrong place in the
  genre tree. A wrong NONE only leaves a tag unmapped, which is today's state. Jev's 10
  wrong genres out of 109 and Claude's 4 out of 115 are the errors a review step must catch.

## Q4: separation, and a better signal

Separation is the ratio of the beam's top path score to the second.

| Measure | Value |
| --- | --- |
| AUROC of separation, right versus wrong | **0.64** (path score 0.70) |
| Median separation, right / wrong | 4.27 / 1.82 |
| Threshold 2.0: auto-accepted precision, review share, queue for >= 5 uses | 90.8%, 34.5%, about 12,250 tags |
| Threshold 5.0 | 92.9%, 58%, about 20,280 tags |

Precision barely rises as the threshold climbs (85.5% with no threshold, 90.8% at 2.0,
92.9% at 5.0), while the review queue grows to most of the vocabulary. Against the draft
labels the AUROC was 0.81, which made separation look useful; the reviewed key shows it is
not. **Separation cannot set an auto-accept threshold.**

Agreement between the two models works better. `evaluate.py` scores this cascade (section
`cascade` in `summary.json`): normalized string match first; on its misses, Jev beam K=3 and
flat Claude each propose.

| Bucket | Sample tags | Right (Jev / Claude) | Est. tags >= 20 uses | Est. tags >= 5 uses |
| --- | --- | --- | --- | --- |
| String match assigns | 43 | 100% (string) | 1,172 | 1,172 |
| Both models name the same genre or style | 40 | **97.5%** / 97.5% | 1,987 | 5,697 |
| Both models say NONE | 80 | 87.5% / 87.5% | 4,546 | 17,797 |
| Models disagree | 37 | 64.9% / 86.5% | 2,174 | 6,414 |

All 10 errors in the "both say NONE" bucket are missed genres, not wrong ones: "composer",
"schubert", "terry riley", "singer", "french singer", "relax", "melodic", "post-whatever",
"star trek", "vomir". Dropping that bucket leaves those tags unmapped, as they are today.
When the models disagree, Claude is right more often, so it should be the reviewer's
default suggestion.

**Review queue:** a person reviews the agreed genre proposals and the disagreements. That is
about **4,200 tags for the >= 20-uses vocabulary** and about 12,100 for >= 5 uses. The sample
is 200 tags, so these estimates carry a wide margin (each sample tag stands for 11 to 530
vocabulary tags, depending on stratum).

## Q5: cost, latency and rate limits

Measured on the 200-tag sample, cold, at list prices read 2026-11-01:

| Measure | Jev `jev-1.14.0`, beam K=3 plus greedy | Flat Claude `claude-opus-5-5` |
| --- | --- | --- |
| Sample cost | $0.042 (657 requests, 1.00M input tokens) | $1.08 (200 requests) |
| Per tag | 3.29 requests, 5,022 input tokens | 1 request, 53 output tokens, cached 10k-token system prompt |
| Latency per request, p50 / p95 | 0.084 s / 0.196 s | 1.79 s / 3.73 s |
| Wall time, whole sample | 5.8 s | 105 s |
| Failed requests | 0 | 0 (no invalid outputs, no refusals) |

Extrapolated full runs:

| Scope | Tags | Jev cost / sequential time | Claude cost |
| --- | --- | --- | --- |
| >= 20 uses | 9,880 | $2.08 / 0.6 h | about $53 |
| >= 5 uses | 31,081 | $6.56 / 2.0 h | $167.58 |
| All distinct | 220,592 | $46.53 / 14 h | $1,189.35 |

The pre-run estimate of 3.3k Jev input tokens per tag was low; the measured figure is 5.0k.
**Caching:** greedy reuses beam's root and genre questions, so running greedy after beam made
0 requests, a 31.7% saving over running both cold. A warm replay of either method makes no
calls, which is what makes a committed capture reproducible. TypeSafe rate limits are set per
organization in its console. The SDK retries 429s and honors `retry-after`, and the sample run
had no failed requests.

Both runs are one-off offline batches, so cost does not decide between them. Jev's value is
as a cheap independent second opinion: agreement with it is what makes Claude's proposals
trustworthy enough to review quickly.

## GO / NO-GO

**GO, for an offline, reviewed lookup.** **NO-GO on separation-based auto-accept.**

The two questions set before the runs:

- *Does beam K=3 find clearly more of the genre-like long tail than normalized string
  matching, at high precision?* Yes: 80.5% against 35.0%. When it assigns, 90.8% is right.
  Flat Claude does better again: 88.6%, and 96.5% right when it assigns.
- *Does separation make the review queue small enough for a person?* No. Model agreement
  does: about 4,200 tags for the >= 20-uses vocabulary, which carries about 95% of tag uses.

The adopted design changes in one way from the provisional outline: confidence comes from
two-model agreement, not from separation. That makes the lookup depend on both TypeSafe and
the Anthropic API, though only in an offline batch.

### ADR outline

- **Lookup:** `design/taxonomy/mb-tag-genre/v1/`, holding `mb-tag-genre.json` (tag ->
  `NONE` | genre | genre/style, with method, source dump date, model ids, both proposals,
  and reviewer), a JSON Schema, and fixtures. This is the same pattern as `media/v1` and
  `company-roles/v1` (ADR 0007/0011).
- **Scope of v1:** tags with >= 20 uses in a named MusicBrainz dump (9,880 tags, about 95% of
  uses). The 5-19-use band triples the review work for about 2% more uses; a later version
  can add it.
- **Order of precedence:**
  1. Normalized string match against Discogs style and genre names. It is accepted without
     review, since it made no wrong assignment in the sample.
  2. Otherwise Jev beam K=3 and flat Claude each propose.
  3. Both say NONE: the tag stays unmapped, with no review.
  4. They agree on a genre or style, or they disagree: a person reviews it, with Claude's
     answer as the default suggestion.

  Unreviewed model rows never ship.
- **Versioning:** a new `vN` when the Discogs tree or the tag vocabulary drifts. The tree is
  frozen from a named dump with the share >= 0.20 rule.
- **Drift check:** a script that diffs the current dump's tags with >= 20 uses against the
  lookup, and lists new tags and tree changes as review work. It runs offline in CI, with no
  model calls.
- **Model dependency:** offline and batch only. It uses pinned versioned models
  (`jev-1.14.0`, `claude-opus-5-5`), never an alias, and the captures are cached and
  committed so a rerun is reproducible. Ingest and the enricher never call a model.

### Follow-up beads (for a replan)

1. A deterministic Discogs tree export (share >= 0.20, releases plus masters) as a versioned
   design artifact.
2. A lookup generator for `mb-tag-genre/v1`: string match first, then the two-model cascade
   over the >= 20-uses vocabulary, writing the review queue. It runs offline with committed
   captures.
3. The review workflow and the person's pass over about 4,200 rows, with Claude's answer as
   the default suggestion.
4. An enricher projection of MusicBrainz tags through the frozen lookup (a plain lookup, no
   model).
5. A drift-check script and a CI job.
6. Optional, later: extend to the 5-19-use band once the v1 review pace is known.

## Reproduce

From `spikes/tag-mapping/`, with the committed caches, these make no network calls:

```sh
python3 string_match.py
uv run --no-project --with typesafe-sdk python jev_beam.py
uv run --no-project --with anthropic python flat_claude.py
python3 evaluate.py
```

`--live` on `jev_beam.py` (with `TYPESAFE_API_KEY`) or `flat_claude.py` (with Anthropic
credentials) allows paid calls on cache misses. **Model pin:** the bead and the cookbook name
`jev-1.12`, but [docs.typesafe.ai/models](https://docs.typesafe.ai/models) lists only
`jev-1.14.0` and `jev-1.13.0`. The harness pins `jev-1.14.0` and aborts if a response reports
another model.
