"""Taxonomy export helper for gm-musicbrainz-graph-enricher-569; works on local files only.

Keeps three things separate and never merges them into a frozen target:
  provenance        where the snapshot came from (required, nonempty)
  observed_pairs    genre/style co-occurrence with counts (JSONL rows: genre, style, count)
  part_of_pairs     actual PART_OF style->genre pairs (JSONL rows: genre, style)
and reports pairs missing from / extra to PART_OF relative to observation, plus styles
observed under several genres. No unobserved link is invented; deciding the target is
a human review step. Provenance with "synthetic": true is propagated as a fixture marker.
"""

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path


RESERVED = {"__NONE__", "__GENRE_ONLY__"}
PROVENANCE_FIELDS = ("source", "snapshot_id", "exported_at")


def read_rows(path):
    rows = []
    with Path(path).open(encoding="utf-8") as stream:
        for number, line in enumerate(stream, 1):
            if line.strip():
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError as error:
                    raise ValueError(f"{path}:{number}: invalid JSON ({error.msg})") from None
    return rows


def pair(row, where):
    if not isinstance(row, dict):
        raise ValueError(f"{where}: row must be an object")
    names = []
    for field in ("genre", "style"):
        value = row.get(field)
        if not isinstance(value, str) or not value.strip() or value in RESERVED:
            raise ValueError(f"{where}: {field} must be a nonempty, non-reserved string")
        names.append(value)  # spelling/case preserved; normalization is a reviewed choice
    return tuple(names)


def build(provenance, observed_rows, part_of_rows):
    if not isinstance(provenance, dict) or any(not provenance.get(field) for field in PROVENANCE_FIELDS):
        raise ValueError(f"provenance requires {', '.join(PROVENANCE_FIELDS)}")
    observed = {}
    for index, row in enumerate(observed_rows):
        key = pair(row, f"observed row {index}")
        count = row.get("count")
        if isinstance(count, bool) or not isinstance(count, int) or count < 1:
            raise ValueError(f"observed row {index}: count must be a positive integer")
        if key in observed:
            raise ValueError(f"observed row {index}: duplicate pair {key}")
        observed[key] = count
    part_of = set()
    for index, row in enumerate(part_of_rows):
        key = pair(row, f"part_of row {index}")
        if key in part_of:
            raise ValueError(f"part_of row {index}: duplicate pair {key}")
        part_of.add(key)
    parents = defaultdict(list)
    for genre, style in observed:
        parents[style].append(genre)
    return {
        "provenance": provenance,
        "synthetic": bool(provenance.get("synthetic")),
        "observed_pairs": [{"genre": g, "style": s, "count": c} for (g, s), c in sorted(observed.items())],
        "part_of_pairs": [{"genre": g, "style": s} for g, s in sorted(part_of)],
        "comparison": {
            "observed_count": len(observed),
            "part_of_count": len(part_of),
            "in_both": len(observed.keys() & part_of),
            "observed_missing_from_part_of": [{"genre": g, "style": s, "count": observed[g, s]} for g, s in sorted(observed.keys() - part_of)],
            "part_of_not_observed": [{"genre": g, "style": s} for g, s in sorted(part_of - observed.keys())],
        },
        "shared_styles": {style: sorted(genres) for style, genres in sorted(parents.items()) if len(genres) > 1},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("provenance", type=Path, help="JSON object: source, snapshot_id, exported_at")
    parser.add_argument("observed", type=Path)
    parser.add_argument("part_of", type=Path)
    args = parser.parse_args()
    try:
        result = build(json.loads(args.provenance.read_text()), read_rows(args.observed), read_rows(args.part_of))
    except ValueError as error:
        sys.exit(f"refused: {error}")
    print(json.dumps(result, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
