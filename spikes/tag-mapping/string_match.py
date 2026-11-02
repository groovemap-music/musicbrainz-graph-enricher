"""Throwaway spike code (gm-musicbrainz-graph-enricher-569). Never imported by the service.

Deterministic baselines: match the tag against Discogs genre and style names.

- ``exact``: case-insensitive equality;
- ``normalized``: lowercase, '&'/'n' -> 'and', every non-alphanumeric character removed
  (so "drum&bass", "Drum n Bass" and "drum and bass" meet).

A genre-name match wins over a style match. A style under several genres goes to the
genre with the most masters carrying that pair. No match predicts NONE. Writes
results/string_match.json.
"""

import json

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


if __name__ == "__main__":
    main()
