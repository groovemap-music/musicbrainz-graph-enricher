"""Throwaway spike code (gm-musicbrainz-graph-enricher-569). Never imported by the service.

Builds the Discogs genre -> style target tree from one masters dump, two ways (Q7):

- ``observed``: every (genre, style) pair carried by any master, with counts and each
  style's share of masters per genre;
- ``part_of``: the product's ``graph.part_of`` rule (database-schema postgres.py::_edge_views),
  restricted to masters: a pair counts only when the master carries exactly one genre.

The tree used for classification keeps a style under a genre when the genre's share of
that style's masters is >= 0.20, the same parent rule ``insights.style_parents`` uses
(analytics-engine insights/style_atlas.py). Every style keeps at least its dominant parent.

Usage: gzip -dc discogs_YYYYMMDD_masters.xml.gz | python discogs_taxonomy.py OUT.json
"""

import json
import sys
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from pathlib import Path


SHARE = 0.20


def main() -> None:
    out = Path(sys.argv[1])
    observed: Counter[tuple[str, str]] = Counter()
    part_of: Counter[tuple[str, str]] = Counter()
    style_masters: Counter[str] = Counter()
    genre_masters: Counter[str] = Counter()
    masters = styled = single_genre = 0
    for _, elem in ET.iterparse(sys.stdin.buffer, events=("end",)):  # noqa: S314
        if elem.tag != "master":
            continue
        masters += 1
        genres = sorted({g.text for g in elem.iterfind("genres/genre") if g.text})
        styles = sorted({s.text for s in elem.iterfind("styles/style") if s.text})
        genre_masters.update(genres)
        style_masters.update(styles)
        styled += bool(styles)
        single_genre += len(genres) == 1
        for g in genres:
            for s in styles:
                observed[g, s] += 1
                if len(genres) == 1:
                    part_of[g, s] += 1
        elem.clear()
    by_style: dict[str, dict[str, int]] = defaultdict(dict)
    for (g, s), n in observed.items():
        by_style[s][g] = n
    tree: dict[str, list[str]] = defaultdict(list)
    multi_parent = 0
    for s, counts in by_style.items():
        total = style_masters[s]
        parents = [g for g, n in counts.items() if n / total >= SHARE] or [max(counts, key=counts.__getitem__)]
        multi_parent += len(parents) > 1
        for g in parents:
            tree[g].append(s)
    result = {
        "source": "Discogs monthly data dump, masters file (data.discogs.com), CC0",
        "counts": {
            "masters": masters,
            "masters_with_styles": styled,
            "single_genre_masters": single_genre,
            "genres": len(genre_masters),
            "styles": len(style_masters),
            "observed_pairs": len(observed),
            "part_of_pairs": len(part_of),
            "observed_pairs_missing_from_part_of": len(set(observed) - set(part_of)),
            "styles_observed_under_2plus_genres": sum(len(c) > 1 for c in by_style.values()),
            "styles_observed_under_5plus_genres": sum(len(c) >= 5 for c in by_style.values()),
            "styles_part_of_2plus_genres": sum(len({g for (g, s2) in part_of if s2 == s}) > 1 for s in by_style),
            "tree_share_threshold": SHARE,
            "tree_pairs": sum(len(v) for v in tree.values()),
            "tree_styles_with_2plus_parents": multi_parent,
        },
        "genre_masters": dict(genre_masters.most_common()),
        "style_masters": dict(style_masters.most_common()),
        "observed": sorted([g, s, n] for (g, s), n in observed.items()),
        "part_of": sorted([g, s, n] for (g, s), n in part_of.items()),
        "tree": {g: sorted(v, key=lambda s: -style_masters[s]) for g, v in sorted(tree.items())},
    }
    out.write_text(json.dumps(result, indent=1, ensure_ascii=False) + "\n")
    sys.stdout.write(json.dumps(result["counts"], indent=1) + "\n")


if __name__ == "__main__":
    main()
