"""Throwaway spike code (gm-musicbrainz-graph-enricher-569). Never imported by the service.

Greedy and beam K=3 traversal of the Discogs tree with TypeSafe Choice questions, after
https://docs.typesafe.ai/cookbooks/hierarchical_classification.md. One Choice per tree
node: the root asks for a genre or NOT A GENRE; each genre asks for one of its styles or
GENRE ONLY. Path score = geometric mean of the decision probabilities; separation = top
path score / second path score.

Every Choice distribution is cached on (model, state, instructions, ordered options) in
data/captures/jev_cache.json, together with the returned model, input tokens and latency.
Replaying with a warm cache makes no calls. Greedy reuses the beam's cached root and
genre questions, which is how cache savings are measured.

Needs TYPESAFE_API_KEY. Run (no project install needed):
  uv run --no-project --with typesafe-sdk python jev_beam.py [--limit N] [--workers 8]
Writes results/jev.json.
"""

import argparse
import hashlib
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

from common import DATA, GENRE_ONLY, HERE, NONE_LABEL, gmean, load_sample, load_tree


MODEL = "jev-1.14.0"
BEAM = 3
CACHE_FILE = DATA / "captures" / "jev_cache.json"
ROOT_INSTRUCTIONS = (
    "`musicbrainz_tag` is a tag that MusicBrainz users applied to an artist, release or label. "
    "Which Discogs music genre does the tag name or belong to? Choose the not-a-genre option when the "
    "tag is not a music genre or style."
)
NONE_TEXT = (
    "Not a music genre or style: a place, nationality, language, year or decade, person or band name, "
    "record label, instrument, voice type, mood, opinion, release format, or personal note"
)


def genre_instructions(genre: str) -> str:
    return (
        f"`musicbrainz_tag` is a tag that MusicBrainz users applied to an artist, release or label, and it "
        f"belongs to the Discogs genre {genre}. Which {genre} style does the tag name? Choose the genre-only "
        f"option when the tag names {genre} itself or no listed style fits."
    )


def genre_only_text(genre: str) -> str:
    return f"{genre} in general: the tag names the genre itself, or none of the listed styles fits"


class Jev:
    def __init__(self, live: bool) -> None:
        self.cache: dict[str, dict] = json.loads(CACHE_FILE.read_text(encoding="utf-8")) if CACHE_FILE.exists() else {}
        self.lock = threading.Lock()
        self.client = None
        self.stats = {"hits": 0, "misses": 0, "failures": 0}
        if live:
            from typesafe_sdk import RetryPolicy, TypeSafeClient  # noqa: PLC0415 - optional, spike only

            self.client = TypeSafeClient(retry=RetryPolicy(max_retries=5, backoff_initial=1.0, backoff_max=20.0))

    def choose(self, tag: str, instructions: str, labels: list[str], descriptions: dict[str, str]) -> dict[str, float]:
        state = {"musicbrainz_tag": tag}
        criteria = {f"c{i}": descriptions.get(label, label) for i, label in enumerate(labels)}
        key = hashlib.sha256(json.dumps([MODEL, state, instructions, criteria], ensure_ascii=False).encode()).hexdigest()
        with self.lock:
            hit = self.cache.get(key)
            if hit is not None:
                self.stats["hits"] += 1
                return dict(zip(labels, hit["p"], strict=True))
            self.stats["misses"] += 1
        if self.client is None:
            raise RuntimeError(f"cache miss for {tag!r} and no live client (--live)")
        from typesafe_sdk import Choice  # noqa: PLC0415

        start = time.perf_counter()
        response = self.client.system_one(state=state, questions={"q": Choice(instructions=instructions, criteria=criteria)}, model=MODEL)
        latency = time.perf_counter() - start
        if response.model != MODEL:
            raise RuntimeError(f"asked for {MODEL}, answered by {response.model}")
        probs = response.answers["q"].probabilities
        record = {
            "tag": tag,
            "parent": instructions[:40],
            "model": response.model,
            "p": [round(float(probs[f"c{i}"]), 6) for i in range(len(labels))],
            "labels_sha": hashlib.sha256("\n".join(labels).encode()).hexdigest()[:12],
            "input_tokens": response.usage.input_tokens,
            "output_tokens": response.usage.output_tokens,
            "latency_s": round(latency, 4),
            "at": datetime.now(UTC).isoformat(timespec="seconds"),
        }
        with self.lock:
            self.cache[key] = record
        return dict(zip(labels, record["p"], strict=True))

    def save(self) -> None:
        CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with self.lock:
            CACHE_FILE.write_text(json.dumps(self.cache, indent=0, ensure_ascii=False) + "\n", encoding="utf-8")


def root(jev: Jev, tree: dict[str, list[str]], tag: str) -> dict[str, float]:
    labels = [*tree, NONE_LABEL]
    return jev.choose(tag, ROOT_INSTRUCTIONS, labels, {NONE_LABEL: NONE_TEXT})


def styles(jev: Jev, tree: dict[str, list[str]], tag: str, genre: str) -> dict[str, float]:
    labels = [*tree[genre], GENRE_ONLY]
    return jev.choose(tag, genre_instructions(genre), labels, {GENRE_ONLY: genre_only_text(genre)})


def to_path(labels: list[str]) -> list[str]:
    return [x for x in labels if x not in (NONE_LABEL, GENRE_ONLY)]


def greedy(jev: Jev, tree: dict[str, list[str]], tag: str) -> dict:
    r = root(jev, tree, tag)
    g = max(r, key=r.__getitem__)
    if g == NONE_LABEL:
        return {"path": [], "score": r[g]}
    s = styles(jev, tree, tag, g)
    best = max(s, key=s.__getitem__)
    return {"path": to_path([g, best]), "score": gmean([r[g], s[best]])}


def beam(jev: Jev, tree: dict[str, list[str]], tag: str, pool: ThreadPoolExecutor) -> dict:
    r = root(jev, tree, tag)
    first = sorted(r.items(), key=lambda kv: (-kv[1], kv[0]))[:BEAM]
    finished = [{"labels": [g], "probs": [p]} for g, p in first if g == NONE_LABEL]
    expand = [(g, p) for g, p in first if g != NONE_LABEL]
    dists = list(pool.map(lambda gp: styles(jev, tree, tag, gp[0]), expand))
    cands = list(finished)
    for (g, p), dist in zip(expand, dists, strict=True):
        cands.extend({"labels": [g, s], "probs": [p, q]} for s, q in dist.items())
    for c in cands:
        c["score"] = gmean(c["probs"])
    cands.sort(key=lambda c: (-c["score"], c["labels"]))
    top = cands[:BEAM]
    second = cands[1]["score"] if len(cands) > 1 else 1e-9
    return {
        "path": to_path(top[0]["labels"]),
        "score": top[0]["score"],
        "separation": top[0]["score"] / max(second, 1e-9),
        "top3": [{"path": to_path(c["labels"]), "score": round(c["score"], 6)} for c in top],
        "root_top3": [[g, round(p, 6)] for g, p in first],
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true", help="allow paid TypeSafe calls on cache misses")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--workers", type=int, default=8)
    args = ap.parse_args()
    tree = load_tree()
    rows = load_sample()[: args.limit or None]
    jev = Jev(args.live)
    out: dict[str, dict] = {}
    inner = ThreadPoolExecutor(max_workers=BEAM * args.workers)

    def run(row: dict) -> None:
        tag = row["tag"]
        t0 = time.perf_counter()
        b = beam(jev, tree, tag, inner)
        t_beam = time.perf_counter() - t0
        t0 = time.perf_counter()
        g = greedy(jev, tree, tag)
        out[tag] = {"beam": b, "greedy": g, "wall_beam_s": round(t_beam, 4), "wall_greedy_after_beam_s": round(time.perf_counter() - t0, 4)}

    t_all = time.perf_counter()
    try:
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            list(pool.map(run, rows))
    finally:
        jev.save()
    wall = time.perf_counter() - t_all
    (HERE / "results").mkdir(exist_ok=True)
    result = {"model": MODEL, "beam_width": BEAM, "wall_s": round(wall, 2), "cache_stats": jev.stats, "tags": out}
    (HERE / "results" / "jev.json").write_text(json.dumps(result, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({"tags": len(out), "wall_s": round(wall, 2), **jev.stats}))


if __name__ == "__main__":
    main()
