"""Throwaway spike code (gm-musicbrainz-graph-enricher-569). Never imported by the service.

Q1/Q2 vocabulary statistics from the MusicBrainz ``mbdump-derived`` tag tables.

Scope matches what musicbrainz-ingestion keeps (artist, label, release, release-group
tags; jsonl_parser.rs). A "use" is one (entity, tag) row with a positive net vote
count, which is what the JSON dumps the pipeline reads expose as ``tags``. Genre-like
share is estimated deterministically against MusicBrainz's own curated genre list
(``/ws/2/genre/all``) plus simple junk patterns; the hand-labelled sample gives the
second, human-checked estimate in the write-up.

Usage: python vocab.py MBDUMP_DIR MB_GENRES_TXT OUT_DIR
Writes OUT_DIR/vocab_stats.json (aggregates) and OUT_DIR/vocab_top.tsv (tag, uses).
"""

import json
import re
import sys
from collections import Counter
from pathlib import Path


SCOPES = ("artist", "label", "release", "release_group")
YEARISH = re.compile(r"^(\d{2,4}s?|'\d0s|\d{4}-\d{2,4})$")
JUNK_WORDS = re.compile(
    r"\b(seen live|favou?rites?|my |owned|wishlist|to listen|check out|awesome|best|love|vinyl|cd|rip|bootleg|"
    r"spotify|amazon|lastfm|last\.fm|albums? i own|under \d|cover|soundtrack of my)\b"
)


def load_tags(root: Path) -> dict[int, str]:
    tags = {}
    with (root / "tag").open(encoding="utf-8") as f:
        for line in f:
            tid, name, _ = line.rstrip("\n").split("\t")
            tags[int(tid)] = name
    return tags


def main() -> None:
    root, genres_txt, out = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
    names = load_tags(root)
    mb_genres = {g.strip().lower() for g in genres_txt.read_text(encoding="utf-8").splitlines() if g.strip()}
    uses: Counter[str] = Counter()
    votes: Counter[str] = Counter()
    per_scope: dict[str, dict[str, int]] = {}
    for scope in SCOPES:
        rows = distinct = 0
        seen: set[int] = set()
        with (root / f"{scope}_tag").open(encoding="utf-8") as f:
            for line in f:
                _, tid, count, _ = line.split("\t")
                c = int(count)
                if c <= 0:
                    continue
                rows += 1
                t = int(tid)
                if t not in seen:
                    seen.add(t)
                    distinct += 1
                name = names[t]
                uses[name] += 1
                votes[name] += c
        per_scope[scope] = {"uses": rows, "distinct_tags": distinct}
    total = sum(uses.values())
    ranked = uses.most_common()
    cover = {}
    for n in (10, 100, 1000, 5000, 10000, 50000):
        cover[f"top_{n}"] = round(sum(c for _, c in ranked[:n]) / total, 4)
    need = {}
    for share in (0.5, 0.8, 0.9, 0.95, 0.99):
        acc = 0
        for i, (_, c) in enumerate(ranked, 1):
            acc += c
            if acc / total >= share:
                need[f"tags_for_{int(share * 100)}pct_uses"] = i
                break

    def bucket(tag: str) -> str:
        if tag in mb_genres:
            return "mb_genre_list"
        if YEARISH.match(tag):
            return "year_or_decade"
        if JUNK_WORDS.search(tag):
            return "personal_or_meta"
        return "other_unlisted"

    by_bucket: dict[str, dict[str, Counter[str]]] = {}
    for label, subset in (("all", ranked), ("top_1000", ranked[:1000]), ("top_10000", ranked[:10000])):
        tags_c: Counter[str] = Counter()
        uses_c: Counter[str] = Counter()
        for tag, c in subset:
            b = bucket(tag)
            tags_c[b] += 1
            uses_c[b] += c
        by_bucket[label] = {"tags": tags_c, "uses": uses_c}
    singletons = sum(1 for _, c in ranked if c == 1)
    stats = {
        "source": "MusicBrainz mbdump-derived.tar.bz2 (fullexport, CC0 for tag tables); see TIMESTAMP in write-up",
        "scopes": list(SCOPES),
        "per_scope": per_scope,
        "distinct_tags_in_tag_table": len(names),
        "distinct_tags_in_scope": len(uses),
        "uses_in_scope": total,
        "vote_weight_in_scope": sum(votes.values()),
        "singleton_tags": singletons,
        "tags_with_5plus_uses": sum(1 for _, c in ranked if c >= 5),
        "tags_with_20plus_uses": sum(1 for _, c in ranked if c >= 20),
        "use_coverage": cover,
        "tags_needed": need,
        "mb_genre_list_size": len(mb_genres),
        "mb_genre_list_present_in_scope": sum(1 for g in mb_genres if g in uses),
        "buckets": by_bucket,
    }
    out.mkdir(parents=True, exist_ok=True)
    (out / "vocab_stats.json").write_text(json.dumps(stats, indent=1) + "\n")
    with (out / "vocab_top.tsv").open("w", encoding="utf-8") as f:
        f.writelines(f"{t}\t{c}\n" for t, c in ranked)
    sys.stdout.write(json.dumps(stats, indent=1) + "\n")


if __name__ == "__main__":
    main()
