# MusicBrainz tag mapping: prerequisite audit and offline preparation

Bead `gm-musicbrainz-graph-enricher-569`. **Blocked, not a completed spike. No
empirical GO / NO-GO verdict.** No provider calls, spend, production changes,
schema changes, or catalog data committed. Preparation is based on independently
integrated main `709616dc8c8af5fd3516e9754675478d26b74f60`.

## Evidence available on 2026-10-03

The local `gm-design-1wd.1/data` entity-resolution cache contains
`mb_20260919-001001.jsonl.gz` (878,061,457 bytes) and
`mb_20260923-001002.jsonl.gz` (879,696,882 bytes). A bounded 1,000-record prefix
audit of **each** found zero `tags` fields, zero tag entries, and 17 other fields.
Both scans were truncated; these zeros are not a measurement of the MusicBrainz
tag vocabulary. The corresponding design spike's `scripts/mb_record.py::compact`
explicitly selects release identity/matching fields and omits tags. Reusing its
entity-match labels would not produce genre/style ground truth.

No genuine approximately 200-tag human-labelled sample was found in the inspected
repository and local spike caches. No configured TypeSafe/Anthropic credential
environment names were present, and no TypeSafe connector was exposed. This does
not establish that credentials or database access cannot exist elsewhere; those
have not been verified. No secrets were read or printed. No local authoritative
tag-bearing corpus or provenance-bearing Discogs taxonomy export was established.
Free disk was approximately 13 GiB; no dump download, unpacking, or broad scan was
attempted.

The [official Python SDK quickstart](https://docs.typesafe.ai/sdk/python) documents
`TYPESAFE_API_KEY` authentication. That variable is absent; `typesafe_sdk` is not
installed in the inspected interpreter, the repository has no `.env`, and no
`~/.config/typesafe` or `~/.typesafe` directories exist. These additional checks
still do not rule out operator-managed credentials elsewhere. No SDK was installed
and no credential store was searched for secret values.

| Required evidence | Current status |
| --- | --- |
| Distinct vocabulary, top-N use coverage | Blocked: inspected extracts omit tags |
| Genre-like/junk prevalence | Blocked: tag-bearing corpus and human classifications absent |
| Greedy / beam K=3 / flat Claude / string-match accuracy | Unmeasured: genuine labels and provider captures absent |
| Separation versus errors; review threshold/queue | Unmeasured; no threshold selected |
| Whole-vocabulary cost, latency, effective rate limits, cache savings | Unmeasured; no live requests or applicable pinned-model billing evidence |
| Observed co-occurrence versus PART_OF coverage | Source policy inspected; actual export/comparison absent |
| Adoption verdict | Withheld until required evidence exists |

The retired product roadmap remains in
`planning-archive/product-roadmap/ROADMAP.md`, lines 276–278: catalog truth,
identity, provenance, and score calculation must not originate from an LLM.
Any eventual proposal must classify **offline distinct tags** into a versioned,
human-reviewed lookup. Ingest would use ordinary lookup only.

## Actual documentation and taxonomy constraints

The [TypeSafe hierarchical-classification cookbook](https://docs.typesafe.ai/cookbooks/hierarchical_classification)
was read directly. It uses Jev 1.12 and greedy versus width-three beam traversal,
geometric means over branching decisions, and top/second score separation.
The [API reference](https://docs.typesafe.ai/api) specifies a complete Choice
distribution. The [current model page](https://docs.typesafe.ai/models) advertises
Jev 1.13; its current pricing and rate limits are not measured Jev 1.12 evidence.
This spike retains `jev-1.12`; access and returned version must be verified before
real capture. Never silently substitute an alias or newer model.

The schema producer's `postgres.py::_edge_views` deliberately emits PART_OF from
release and master documents only when exactly one genre is present. Multi-genre
co-occurrence is broader but does not establish a unique parent. Neither policy
is changed here. A future taxonomy export must preserve separately: source
snapshot/provenance, observed genre/style pairs with counts, and actual PART_OF
pairs. Compare missing/extra pairs and human-review ambiguous associations before
freezing the target. Shared styles remain separate full `(genre, style)` paths.
The root includes NONE and each genre includes a genre-only terminal; a style-only
label cannot distinguish parents. No unobserved links are invented.

## Reproducible offline preparation

`scripts/spike_tag_mapping.py` uses only Python's standard library and cannot call
a network/provider. It is excluded from the application wheel. Examples:

```sh
uv run python scripts/spike_tag_mapping.py inventory /local/tag-bearing.jsonl.gz --max-records 1000
uv run python scripts/spike_tag_mapping.py replay /local/reviewed-evidence.json
uv run pytest tests/test_tag_mapping_spike.py
```

Inventory emits aggregate counts only: record/tag-field presence, distinct literal
tags, occurrence counts, separately counted nonnegative MusicBrainz votes, and
top-10/100/1000 occurrence coverage. It reports truncation and leaves genre-like
share null. It does not normalize case/spelling, classify junk, export tags, or
claim a bounded prefix represents the entire corpus. To reproduce the audit,
pass each named local cache above with `--max-records 1000`.

Replay's JSON bundle contains:

- `taxonomy`: `genres` mapping each genre to its style names, and nonempty
  `provenance`. Its canonical JSON SHA-256 identifies the target revision.
- `labels`: distinct rows with `tag`, `accepted_paths`, `reviewed_by`,
  `reviewed_at`, and `source`. NONE is `[[]]`; genre-only is `[["genre"]]`.
  Multiple acceptable full paths can represent a genuinely ambiguous annotation.
- `captures`: mapping `request_key(tag, children(tree, parent), taxonomy_sha,
  parent)` to `model` and complete child-label `probabilities`, from actual Jev
  1.12 responses. Root options include `__NONE__`; genre children include
  `__GENRE_ONLY__`. Preserve ordered options, parent context, exact prompt and
  taxonomy revision in the real request; a shared style's context must not collide
  across genres. Retain raw request/response and measurement provenance locally.
- `flat_claude`: tag-keyed actual records with immutable `model`, `capture_source`,
  and full target `path`. Supply the same frozen target set including NONE and
  genre-only, not an independently invented list.

Human provenance fields are required but cannot authenticate a reviewer. Synthetic
test attestations/captures are visibly fixtures, not acceptance evidence. The
replay rejects missing distributions/model mismatches; zeros do not become tiny
positive paths. Single-child edges do not increase confidence. Ties sort by full
path. Case-insensitive exact string matching abstains when ambiguous or unmatched;
an abstention counts as incorrect, even for NONE, to avoid inflated junk accuracy.
All four methods use the same sample denominator. A separation sweep reports
accepted counts/errors and review counts; singleton surviving paths go to review.
It never selects a threshold, estimates billing, or produces an adoption verdict.

The synthetic regressions cover early-choice beam recovery, shared-style context,
genre-only scoring, invalid/missing distributions, model mismatch, cache identity,
bounded field/occurrence/vote accounting, unreviewed labels, and common evaluation
denominators. These verify preparation only.

Validation: the shared-admission `bh work check` ran the complete `just check`
successfully: 267 tests passed, three live integration tests deselected, 98% runtime
coverage, formatting/lint/contracts/type checking, secret scans, wheel build and
production install, license policy, and bump preview. Tests used the existing
Python 3.14.5 environment; production installation used pinned Python 3.14.7.
Nine of the passing tests are the synthetic preparation regressions. Passing this
gate does not satisfy the missing empirical spike acceptance, so no submission is
made.

## Offline tooling added since the audit

All stdlib-only, excluded from the wheel, exercised only by fake in-process
transports and synthetic fixtures (`tests/test_tag_capture_spike.py`). None of it is
acceptance evidence.

- `scripts/spike_tag_capture.py capture`: a provider-agnostic capture adapter. It
  needs `TYPESAFE_API_KEY` in the environment and a caller-supplied transport
  (`--transport-factory module:callable`; see the module docstring for the two-method
  contract). It refuses to start when the key is absent or when `jev-1.12` cannot be
  verified, and aborts (caching nothing) if a response reports another model; it
  never substitutes an alias or newer model. Requests are bounded (`--max-requests`
  counts every attempt, including retries). Rate-limit responses are retried after the
  provider's own retry-after, or capped exponential backoff when none is given, and
  a retry-after beyond the bound becomes a recorded failure instead of a long sleep.
  Output is the replay `taxonomy` + `captures` shape plus `measurements` (exact
  returned model, wall-clock latency, token usage, attempts), `failures` (redacted),
  and completed/failed/skipped tags. The key is never written. Labels and
  `flat_claude` must still be added by people/actual runs before `replay`.
- The cache is the `captures` mapping itself, keyed by the replay's `request_key`
  (tag, options, taxonomy sha, parent, prompt, model). Passing a prior bundle back in
  resumes without calls; stats report cold versus replay request counts, latency,
  and tokens avoided. `traverse` gained an optional `resolve` hook for this; replay
  behaviour is unchanged.
- `scripts/spike_tag_capture.py extrapolate`: projects requests, tokens and
  sequential latency from a capture's measured usage and an explicitly supplied
  actual vocabulary size from a full inventory. It refuses without measurements, usage,
  or size. A cost is produced only if you supply rates with their authoritative
  source; otherwise cost is null. It assumes linearity and no concurrency, and
  states so.
- `scripts/spike_taxonomy_export.py`: given local provenance JSON and two JSONL files
  (observed `genre, style, count`; actual PART_OF `genre, style`), it keeps both pair
  sets and provenance separate, reports observed pairs missing from PART_OF, PART_OF
  pairs never observed, and styles observed under several genres. It never merges or
  freezes a target.

## Remaining blockers (all need a person, credentials, or data)

1. A real transport. The TypeSafe wire format was not verifiable from this seat
   (no Context7 access; no SDK installed; no network used), so no transport ships.
   Someone with SDK/docs access must write one for the contract above and confirm
   that it reports the exact returned model, complete Choice probabilities and usage.
2. `TYPESAFE_API_KEY`, verified access to `jev-1.12` (not the advertised 1.13), and a
   spend budget, plus authoritative billing rates for that model.
3. A tag-bearing MusicBrainz corpus with release versus release-group scope and vote
   semantics documented, to run `inventory` over fully for vocabulary size and coverage.
4. Real observed and PART_OF exports from the Discogs-side store to feed the taxonomy
   export, followed by human review of ambiguous parents.
5. Approximately 200 human-labelled tags (head/long-tail, ambiguous parents,
   non-English, genre-only, junk) with real reviewer provenance and a calibration /
   evaluation split. Never model-generated.
6. Flat-Claude captures against the same frozen target set.
7. Run the four-method evaluation, separation analysis and extrapolation on real
   inputs, then the GO / NO-GO. Until then the spike is **not complete** and no verdict
   exists.

