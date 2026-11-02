"""Throwaway spike code (gm-musicbrainz-graph-enricher-569). Never imported by the service.

Flat baseline: one Claude request per tag that picks a single option from the whole
flattened target list (NONE, every genre-only option, every genre/style leaf). The list
sits in a cached system prompt; the tag is the only per-request content. Output is
constrained to {"option": <int>} by a JSON schema.

Answers are cached per (model, effort, tag, list sha) in data/captures/claude_cache.json
with usage and latency, so a warm replay makes no calls.

Needs Anthropic credentials. Run (no project install needed):
  uv run --no-project --with anthropic python flat_claude.py --live [--limit N] [--workers 4]
Writes results/flat_claude.json.
"""

import argparse
import hashlib
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

from common import DATA, HERE, all_paths, fmt_path, load_sample, load_tree


MODEL = "claude-opus-5-5"
EFFORT = "medium"
CACHE_FILE = DATA / "captures" / "claude_cache.json"


def system_prompt(options: list[tuple[str, ...]]) -> str:
    lines = [
        f"{i}\t{'NONE (not a music genre or style)' if not p else ' / '.join(p) + ('' if len(p) == 2 else ' (genre only)')}"
        for i, p in enumerate(options)
    ]
    return (
        "You map MusicBrainz user tags onto the Discogs genre/style taxonomy.\n"
        "A tag was applied by MusicBrainz users to an artist, release or label. Pick exactly one option id:\n"
        "- a 'Genre / Style' option when the tag names that style (or a close equivalent of it);\n"
        "- a 'Genre (genre only)' option when the tag names the genre itself, or a genre-like sound in that genre "
        "that none of its styles matches;\n"
        "- option 0 (NONE) when the tag is not a music genre or style: a place, nationality, language, year or "
        "decade, person or band name, record label, instrument, voice type, mood, opinion, release format, or "
        "personal note.\n"
        "Many styles appear under more than one genre; pick the genre that fits the tag best.\n\n"
        "Options (id<TAB>option):\n" + "\n".join(lines)
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--workers", type=int, default=4)
    args = ap.parse_args()
    tree = load_tree()
    options = all_paths(tree)
    system = system_prompt(options)
    list_sha = hashlib.sha256(system.encode()).hexdigest()[:16]
    cache: dict[str, dict] = json.loads(CACHE_FILE.read_text(encoding="utf-8")) if CACHE_FILE.exists() else {}
    lock = threading.Lock()
    stats = {"hits": 0, "misses": 0, "invalid": 0, "refusals": 0}
    client = None
    if args.live:
        import anthropic  # noqa: PLC0415 - optional, spike only

        client = anthropic.Anthropic()
    schema = {
        "type": "object",
        "properties": {"option": {"type": "integer"}},
        "required": ["option"],
        "additionalProperties": False,
    }

    def ask(tag: str) -> dict:
        key = hashlib.sha256(json.dumps([MODEL, EFFORT, list_sha, tag], ensure_ascii=False).encode()).hexdigest()
        with lock:
            if key in cache:
                stats["hits"] += 1
                return cache[key]
            stats["misses"] += 1
        if client is None:
            raise RuntimeError(f"cache miss for {tag!r} and no live client (--live)")
        start = time.perf_counter()
        response = client.beta.messages.create(
            model=MODEL,
            max_tokens=8000,
            betas=["server-side-fallback-2026-07-01"],
            fallbacks="default",
            system=[{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}],
            output_config={"effort": EFFORT, "format": {"type": "json_schema", "schema": schema}},
            messages=[{"role": "user", "content": f"MusicBrainz tag: {json.dumps(tag, ensure_ascii=False)}"}],
        )
        latency = time.perf_counter() - start
        option = None
        if response.stop_reason == "refusal":
            with lock:
                stats["refusals"] += 1
        else:
            text = next((b.text for b in response.content if b.type == "text"), "")
            try:
                option = int(json.loads(text)["option"])
            except ValueError, KeyError, TypeError:
                option = None
        u = response.usage
        record = {
            "tag": tag,
            "model": response.model,
            "effort": EFFORT,
            "stop_reason": response.stop_reason,
            "option": option,
            "path": list(options[option]) if option is not None and 0 <= option < len(options) else None,
            "input_tokens": u.input_tokens,
            "cache_creation_input_tokens": u.cache_creation_input_tokens or 0,
            "cache_read_input_tokens": u.cache_read_input_tokens or 0,
            "output_tokens": u.output_tokens,
            "latency_s": round(latency, 4),
            "at": datetime.now(UTC).isoformat(timespec="seconds"),
        }
        if record["path"] is None:
            with lock:
                stats["invalid"] += 1
        with lock:
            cache[key] = record
        return record

    rows = load_sample()[: args.limit or None]
    # Warm the prompt cache with one request before fanning out.
    t0 = time.perf_counter()
    try:
        first = [ask(rows[0]["tag"])]
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            rest = list(pool.map(lambda r: ask(r["tag"]), rows[1:]))
    finally:
        CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        CACHE_FILE.write_text(json.dumps(cache, indent=0, ensure_ascii=False) + "\n", encoding="utf-8")
    wall = time.perf_counter() - t0
    out = {r["tag"]: rec["path"] for r, rec in zip(rows, first + rest, strict=True)}
    (HERE / "results").mkdir(exist_ok=True)
    result = {"model": MODEL, "effort": EFFORT, "options": len(options), "list_sha": list_sha, "wall_s": round(wall, 2), "stats": stats, "paths": out}
    (HERE / "results" / "flat_claude.json").write_text(json.dumps(result, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({"tags": len(out), "wall_s": round(wall, 2), **stats, "example": fmt_path(tuple(out[rows[0]["tag"]] or ()))}))


if __name__ == "__main__":
    main()
