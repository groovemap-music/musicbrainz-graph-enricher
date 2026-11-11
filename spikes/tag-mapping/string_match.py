"""Throwaway spike code (gm-musicbrainz-graph-enricher-569). Never imported by the service.

Deterministic baselines: match the tag against Discogs genre and style names.

- ``exact``: case-insensitive equality;
- ``normalized``: lowercase, '&'/'n' -> 'and', every non-alphanumeric character removed
  (so "drum&bass", "Drum n Bass" and "drum and bass" meet).

A genre-name match wins over a style match. A style under several genres goes to the
genre with the most masters carrying that pair. No match predicts NONE. Writes
results/string_match.json; given vocab_top.tsv also the whole-vocabulary coverage.
"""

import json
import sys
from pathlib import Path

from common import DATA, HERE, load_sample, load_tree, norm


def build(tree: dict[str, list[str]], key) -> dict[str, tuple[str, ...]]:
    pairs = json.loads((DATA / "discogs_pairs_20261101.json").read_text(encoding="utf-8"))["observed"]
    weight = {(g, s): n for g, s, n in pairs}
    table: dict[str, tuple[str, ...]] = {}
    for g, styles in tree.items():
        for s in styles:
            k = key(s)
            cur = table.get(k)
            if cur is None or weight.get((g, s), 0) > weight.get(cur, 0):
                table[k] = (g, s)
    for g in tree:
        table[key(g)] = (g,)
    return table


def main() -> None:
    tree = load_tree()
    out = {}
    for name, key in (("exact", lambda x: x.strip().lower()), ("normalized", norm)):
        table = build(tree, key)
        out[name] = {r["tag"]: list(table.get(key(r["tag"]), ())) for r in load_sample()}
    (HERE / "results").mkdir(exist_ok=True)
    (HERE / "results" / "string_match.json").write_text(json.dumps(out, indent=1, ensure_ascii=False) + "\n")
    if len(sys.argv) > 1:  # optional: vocab_top.tsv from vocab.py, for whole-vocabulary coverage
        rows = [(t, int(c)) for t, c in (line.rstrip("\n").split("\t") for line in Path(sys.argv[1]).read_text(encoding="utf-8").splitlines())]
        total = sum(c for _, c in rows)
        cover = {}
        for name, key in (("exact", lambda x: x.strip().lower()), ("normalized", norm)):
            table = build(tree, key)
            hit = [(t, c) for t, c in rows if key(t) in table]
            cover[name] = {"tags": len(hit), "use_share": round(sum(c for _, c in hit) / total, 4)}
        (HERE / "results" / "string_match_vocab_coverage.json").write_text(json.dumps(cover, indent=1) + "\n")
        print(json.dumps(cover))


if __name__ == "__main__":
    main()
