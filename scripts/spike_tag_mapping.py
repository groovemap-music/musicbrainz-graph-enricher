"""Offline preparation for gm-musicbrainz-graph-enricher-569; never calls a provider.

Inventory reads local JSONL(.gz). Replay consumes externally collected Choice
distributions; synthetic fixtures verify algorithms, never stand in for evidence.
"""

import argparse
import gzip
import hashlib
import json
import math
from collections import Counter
from pathlib import Path


MODEL = "jev-1.12"
NONE = "__NONE__"
GENRE_ONLY = "__GENRE_ONLY__"
INSTRUCTIONS = "Map the MusicBrainz tag to a direct child; choose NONE for non-genre tags and GENRE_ONLY when no style applies."


def inventory(path, limit):
    """Count occurrences, separately from MusicBrainz vote weights; emit no tags."""
    if limit < 1:
        raise ValueError("a positive bounded record limit is required")
    opener = gzip.open if path.suffix == ".gz" else Path.open
    vocabulary = Counter()
    fields = tagged = votes = invalid = records = 0
    truncated = False
    with opener(path, "rt", encoding="utf-8") as stream:
        for index, line in enumerate(stream):
            if index == limit:
                truncated = True
                break
            row = json.loads(line)
            records += 1
            fields += "tags" in row
            tags = row.get("tags") or []
            if not isinstance(tags, list):
                invalid += 1
                continue
            tagged += bool(tags)
            for tag in tags:
                if not isinstance(tag, dict) or not isinstance(tag.get("name"), str) or not tag["name"].strip():
                    invalid += 1
                    continue
                vocabulary[tag["name"]] += 1  # Preserve spelling/case; normalization is a separate reviewed choice.
                count = tag.get("count")
                if isinstance(count, int) and not isinstance(count, bool) and count >= 0:
                    votes += count
    uses = sum(vocabulary.values())
    return {
        "records": records,
        "truncated": truncated,
        "records_with_tags_field": fields,
        "tagged_records": tagged,
        "invalid_tag_entries": invalid,
        "distinct_tags": len(vocabulary),
        "tag_occurrences": uses,
        "nonnegative_vote_sum": votes,
        "coverage_by_top_n": {str(n): sum(v for _, v in vocabulary.most_common(n)) / uses if uses else None for n in (10, 100, 1000)},
        "genre_like_share": None,  # Requires actual human review, not a keyword heuristic.
    }


def children(tree, path):
    if not path:
        return [*sorted(tree), NONE]
    if len(path) == 1 and path[0] != NONE:
        return [*sorted(tree[path[0]]), GENRE_ONLY]
    return []


def request_key(tag, options, taxonomy_sha, parent=()):
    """Order, prompt, taxonomy, and pinned model all participate in cache identity."""
    payload = {"tag": tag, "parent": parent, "options": options, "taxonomy_sha": taxonomy_sha, "instructions": INSTRUCTIONS, "model": MODEL}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def traverse(tag, tree, captures, taxonomy_sha, width):
    if width not in (1, 3):
        raise ValueError("only greedy and beam K=3 are prepared")
    candidates = [((), 0.0, 0)]
    for _ in range(2):
        expanded = []
        for path, log_probability, decisions in candidates:
            options = children(tree, path)
            if not options:
                expanded.append((path, log_probability, decisions))
                continue
            if len(options) == 1:
                expanded.append(((*path, options[0]), log_probability, decisions))
                continue
            capture = captures[request_key(tag, options, taxonomy_sha, path)]  # Missing evidence fails closed.
            if capture["model"] != MODEL and not capture["model"].startswith(MODEL + "."):
                raise ValueError("capture did not use the required Jev 1.12 model")
            probabilities = capture["probabilities"]
            if set(probabilities) != set(options) or any(not math.isfinite(p) or not 0 <= p <= 1 for p in probabilities.values()):
                raise ValueError("capture must contain the complete valid child distribution")
            if not math.isclose(sum(probabilities.values()), 1.0, abs_tol=1e-6):
                raise ValueError("probabilities must sum to one")
            for option in options:
                if probabilities[option] == 0:
                    continue
                expanded.append(((*path, option), log_probability + math.log(probabilities[option]), decisions + 1))
        candidates = sorted(expanded, key=lambda item: (-(item[1] / item[2] if item[2] else 0), item[0]))[:width]
    return [{"path": list(path), "score": math.exp(logp / count) if count else 1.0} for path, logp, count in candidates]


def leaf(path):
    return [] if path == [NONE] else [part for part in path if part != GENRE_ONLY]


def evaluate(bundle):
    """Replay labelled evidence. These attestations cannot authenticate a human."""
    tree = bundle["taxonomy"]["genres"]
    if not bundle["taxonomy"].get("provenance"):
        raise ValueError("taxonomy provenance is required")
    if NONE in tree or any(GENRE_ONLY in styles or NONE in styles or len(styles) != len(set(styles)) for styles in tree.values()):
        raise ValueError("taxonomy contains reserved or duplicate labels")
    taxonomy_sha = hashlib.sha256(json.dumps(bundle["taxonomy"], sort_keys=True).encode()).hexdigest()
    labels = bundle["labels"]
    if not labels or len({row["tag"] for row in labels}) != len(labels):
        raise ValueError("nonempty distinct-tag labelled sample required")
    rows = []
    correct = Counter()
    for row in labels:
        if any(not row.get(field) for field in ("reviewed_by", "reviewed_at", "source")):
            raise ValueError("human label provenance is required")
        expected = row["accepted_paths"]
        if not expected:
            raise ValueError("NONE is an empty path inside accepted_paths, not missing labels")
        for path in expected:
            if path and (path[0] not in tree or len(path) > 2 or (len(path) == 2 and path[1] not in tree[path[0]])):
                raise ValueError("human target outside taxonomy")
        greedy = traverse(row["tag"], tree, bundle["captures"], taxonomy_sha, 1)
        beam = traverse(row["tag"], tree, bundle["captures"], taxonomy_sha, 3)
        matches = [[genre, style] for genre, styles in tree.items() for style in styles if style.casefold() == row["tag"].casefold()]
        matches += [[genre] for genre in tree if genre.casefold() == row["tag"].casefold()]
        # Ambiguous string matches abstain, rather than selecting an arbitrary genre.
        string_path = matches[0] if len(matches) == 1 else None
        flat = bundle["flat_claude"][row["tag"]]
        if not flat.get("model", "").startswith("claude") or not flat.get("capture_source"):
            raise ValueError("actual flat-Claude model and capture provenance required")
        predictions = {"greedy": leaf(greedy[0]["path"]), "beam_k3": leaf(beam[0]["path"]), "flat_claude": flat["path"], "string_match": string_path}
        for method, prediction in predictions.items():
            correct[method] += prediction in expected
        separation = beam[0]["score"] / beam[1]["score"] if len(beam) > 1 else None
        rows.append({"separation": separation, "beam_correct": predictions["beam_k3"] in expected})
    return {
        "sample_size": len(labels),
        "accuracy": {method: correct[method] / len(labels) for method in ("greedy", "beam_k3", "flat_claude", "string_match")},
        "threshold_sweep": [
            {
                "threshold": threshold,
                "accepted": sum(row["separation"] is not None and row["separation"] >= threshold for row in rows),
                "accepted_errors": sum(row["separation"] is not None and row["separation"] >= threshold and not row["beam_correct"] for row in rows),
                "review": sum(row["separation"] is None or row["separation"] < threshold for row in rows),
            }
            for threshold in (1.0, 1.5, 2.0, 3.0, 5.0, 10.0)
        ],
        "chosen_auto_accept_threshold": None,
        "provider_cost_latency_rate": None,
        "verdict": None,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    scan = sub.add_parser("inventory")
    scan.add_argument("corpus", type=Path)
    scan.add_argument("--max-records", type=int, default=1000)
    replay = sub.add_parser("replay")
    replay.add_argument("bundle", type=Path)
    args = parser.parse_args()
    result = inventory(args.corpus, args.max_records) if args.command == "inventory" else evaluate(json.loads(args.bundle.read_text()))
    print(json.dumps(result, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
