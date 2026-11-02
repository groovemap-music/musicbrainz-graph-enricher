"""Throwaway spike code (gm-musicbrainz-graph-enricher-569). Never imported by the service.

Shared loaders, the label key, and scoring for the four methods.

A path is a tuple: () is NONE (not a genre), (genre,) is genre-only, (genre, style) a leaf.
"""

import json
import math
import re
from pathlib import Path


HERE = Path(__file__).resolve().parent
DATA = HERE / "data"
TREE_FILE = DATA / "discogs_tree_20261101.json"
NONE_LABEL = "__NONE__"
GENRE_ONLY = "__GENRE_ONLY__"

Path_ = tuple[str, ...]


def load_tree() -> dict[str, list[str]]:
    tree: dict[str, list[str]] = json.loads(TREE_FILE.read_text(encoding="utf-8"))["tree"]
    return tree


def genre_masters() -> dict[str, int]:
    counts: dict[str, int] = json.loads(TREE_FILE.read_text(encoding="utf-8"))["genre_masters"]
    return counts


def parents(tree: dict[str, list[str]]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for g, styles in tree.items():
        for s in styles:
            out.setdefault(s, []).append(g)
    return out


def all_paths(tree: dict[str, list[str]]) -> list[Path_]:
    paths: list[Path_] = [()]
    for g, styles in tree.items():
        paths.append((g,))
        paths.extend((g, s) for s in styles)
    return paths


def load_sample() -> list[dict]:
    rows: list[dict] = json.loads((DATA / "sample.json").read_text(encoding="utf-8"))["rows"]
    return rows


def load_labels(tree: dict[str, list[str]]) -> dict[str, set[Path_]]:
    """Expand labels.tsv into accepted path sets, rejecting anything not in the tree."""
    par = parents(tree)
    labels: dict[str, set[Path_]] = {}
    lines = (DATA / "labels.tsv").read_text(encoding="utf-8").splitlines()[1:]
    for line in lines:
        tag, spec = line.split("\t")[:2]
        accepted: set[Path_] = set()
        for item in (x.strip() for x in spec.split(" | ")):
            if item == "NONE":
                accepted.add(())
            elif item.startswith("G:"):
                g = item[2:]
                if g not in tree:
                    raise ValueError(f"{tag}: unknown genre {g}")
                accepted.add((g,))
            elif item.startswith("S:"):
                s = item[2:]
                if s not in par:
                    raise ValueError(f"{tag}: unknown style {s}")
                accepted.update((g, s) for g in par[s])
            elif item.startswith("P:"):
                g, s = item[2:].split("/", 1)
                if s not in tree.get(g, []):
                    raise ValueError(f"{tag}: {g}/{s} not in tree")
                accepted.add((g, s))
            else:
                raise ValueError(f"{tag}: bad item {item}")
        labels[tag] = accepted
    return labels


def norm(text: str) -> str:
    text = text.lower().replace("&", " and ").replace("'n'", " and ").replace(" n ", " and ")
    return re.sub(r"[^0-9a-zÀ-￿]+", "", text)


def score(pred: Path_, accepted: set[Path_]) -> dict[str, bool]:
    """exact: an accepted path; genre: same top-level decision (NONE counts as a genre)."""
    accepted_genres = {p[:1] for p in accepted}
    return {"exact": pred in accepted, "genre": pred[:1] in accepted_genres}


def gmean(probs: list[float]) -> float:
    return math.exp(sum(math.log(max(p, 1e-9)) for p in probs) / len(probs)) if probs else 1.0


def fmt_path(p: Path_) -> str:
    return "NONE" if not p else "/".join(p)
