"""Throwaway spike code (gm-musicbrainz-graph-enricher-569). Never imported by the service.

Scores every method present in results/ against data/labels.tsv and writes
results/summary.json: accuracy (overall, by stratum, genre-like vs junk, use-weighted,
stratum-weighted), the beam separation sweep with review-queue sizes, and cost/latency
extrapolations from the cached measurements. Makes no network calls.

Usage: python evaluate.py
"""

import json
import statistics

from common import DATA, HERE, fmt_path, load_labels, load_sample, load_tree, score


RES = HERE / "results"
# Published list prices, read 2026-11-01: https://docs.typesafe.ai/models (Jev, input only,
# output free) and the claude-api skill's model table (Claude Opus 5.5).
JEV_USD_PER_MTOK_IN = 0.042
OPUS_IN, OPUS_CACHE_WRITE, OPUS_CACHE_READ, OPUS_OUT = 4.00, 5.00, 0.20, 20.00
THRESHOLDS = [1.0, 1.1, 1.25, 1.5, 2, 3, 5, 10, 20, 50, 100]


def load(name: str) -> dict | None:
    p = RES / name
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def predictions() -> dict[str, dict[str, tuple[str, ...]]]:
    preds: dict[str, dict[str, tuple[str, ...]]] = {}
    sm = load("string_match.json")
    if sm:
        for variant, table in sm.items():
            preds[f"string_{variant}"] = {t: tuple(p) for t, p in table.items()}
    jev = load("jev.json")
    if jev:
        preds["jev_greedy"] = {t: tuple(r["greedy"]["path"]) for t, r in jev["tags"].items()}
        preds["jev_beam3"] = {t: tuple(r["beam"]["path"]) for t, r in jev["tags"].items()}
    fc = load("flat_claude.json")
    if fc:
        preds["flat_claude"] = {t: tuple(p) if p is not None else ("__INVALID__",) for t, p in fc["paths"].items()}
    return preds


def auroc(pos: list[float], neg: list[float]) -> float | None:
    if not pos or not neg:
        return None
    wins = sum((p > n) + 0.5 * (p == n) for p in pos for n in neg)
    return round(wins / (len(pos) * len(neg)), 4)


def pct(xs: list[float], q: float) -> float:
    xs = sorted(xs)
    return xs[min(len(xs) - 1, round(q * (len(xs) - 1)))]


def main() -> None:
    tree = load_tree()
    labels = load_labels(tree)
    rows = load_sample()
    sizes = {r["stratum"]: r["stratum_size"] for r in rows}
    n_by = {s: sum(r["stratum"] == s for r in rows) for s in sizes}
    junk = {t for t, a in labels.items() if a == {()}}
    genre_like = {t for t, a in labels.items() if () not in a}
    either = set(labels) - junk - genre_like
    vocab = json.loads((DATA / "vocab_stats.json").read_text(encoding="utf-8"))
    summary: dict = {
        "sample": {
            "n": len(rows),
            "strata": {s: {"sampled": n_by[s], "population": sizes[s]} for s in sizes},
            "labels_none_only": len(junk),
            "labels_genre_only_or_style": len(genre_like),
            "labels_none_or_genre_accepted": len(either),
        }
    }
    # Q2 from the labelled sample: share of tags (and uses) that are junk, per stratum.
    q2 = {}
    for s in sizes:
        rs = [r for r in rows if r["stratum"] == s]
        q2[s] = {
            "junk_share_tags": round(sum(r["tag"] in junk for r in rs) / len(rs), 3),
            "genre_like_share_tags": round(sum(r["tag"] in genre_like for r in rs) / len(rs), 3),
            "junk_share_uses": round(sum(r["uses"] for r in rs if r["tag"] in junk) / sum(r["uses"] for r in rs), 3),
        }
    summary["q2_from_labels"] = q2

    methods = {}
    for name, pred in predictions().items():
        res = {t: score(pred.get(t, ("__MISSING__",)), labels[t]) for t in labels}
        by_tag = {r["tag"]: r for r in rows}

        def acc(tags: set[str] | list[str], key: str = "exact", res=res) -> float | None:
            tags = list(tags)
            return round(sum(res[t][key] for t in tags) / len(tags), 3) if tags else None

        strat = {s: acc([t for t in labels if by_tag[t]["stratum"] == s]) for s in sizes}
        weighted = sum(strat[s] * sizes[s] for s in sizes) / sum(sizes.values())
        uses_w = sum(by_tag[t]["uses"] * res[t]["exact"] for t in labels) / sum(by_tag[t]["uses"] for t in labels)
        pred_none = {t for t in labels if pred.get(t) == ()}
        true_none_ok = {t for t in labels if () in labels[t]}
        methods[name] = {
            "exact": acc(labels),
            "genre_level": acc(labels, "genre"),
            "exact_correct": sum(r["exact"] for r in res.values()),
            "genre_like_exact": acc(genre_like),
            "genre_like_genre_level": acc(genre_like, "genre"),
            "junk_exact": acc(junk),
            "ambiguous_exact": acc(either),
            "by_stratum": strat,
            "stratum_weighted_exact": round(weighted, 3),
            "use_weighted_exact": round(uses_w, 3),
            "none_precision": round(len(pred_none & true_none_ok) / len(pred_none), 3) if pred_none else None,
            "none_recall_on_junk": round(len(pred_none & junk) / len(junk), 3),
            "assigned": len(labels) - len(pred_none),
            "assigned_precision": round(sum(res[t]["exact"] for t in labels if t not in pred_none) / (len(labels) - len(pred_none)), 3)
            if len(labels) > len(pred_none)
            else None,
            "genre_like_recall": round(sum(res[t]["exact"] for t in genre_like if t not in pred_none) / len(genre_like), 3),
            "junk_given_a_genre": sorted(t for t in junk if t not in pred_none),
            "errors": sorted(f"{t} -> {fmt_path(pred.get(t, ()))}" for t in labels if not res[t]["exact"]),
        }
    summary["methods"] = methods

    jev = load("jev.json")
    if jev:
        tags = jev["tags"]
        res = {t: score(tuple(tags[t]["beam"]["path"]), labels[t])["exact"] for t in labels}
        seps = {t: tags[t]["beam"]["separation"] for t in labels}
        scores = {t: tags[t]["beam"]["score"] for t in labels}
        sweep = []
        for th in THRESHOLDS:
            acc_t = [t for t in labels if seps[t] >= th]
            rev = [t for t in labels if seps[t] < th]
            wrong = [t for t in acc_t if not res[t]]
            est_review = sum(sizes[s] * sum(1 for t in rev if next(r for r in rows if r["tag"] == t)["stratum"] == s) / n_by[s] for s in sizes)
            sweep.append(
                {
                    "threshold": th,
                    "auto_accepted": len(acc_t),
                    "auto_accepted_errors": len(wrong),
                    "auto_accepted_precision": round(1 - len(wrong) / len(acc_t), 3) if acc_t else None,
                    "review_queue": len(rev),
                    "review_share": round(len(rev) / len(labels), 3),
                    "errors_caught": sum(1 for t in rev if not res[t]),
                    "est_review_queue_tags_ge5_uses": round(est_review),
                }
            )
        summary["separation"] = {
            "auroc_separation": auroc([seps[t] for t in labels if res[t]], [seps[t] for t in labels if not res[t]]),
            "auroc_path_score": auroc([scores[t] for t in labels if res[t]], [scores[t] for t in labels if not res[t]]),
            "median_separation_correct": round(statistics.median([seps[t] for t in labels if res[t]]), 3),
            "median_separation_wrong": round(statistics.median([seps[t] for t in labels if not res[t]]), 3)
            if any(not v for v in res.values())
            else None,
            "sweep": sweep,
        }
        cache = json.loads((DATA / "captures" / "jev_cache.json").read_text(encoding="utf-8"))
        recs = list(cache.values())
        per_tag_tokens: dict[str, int] = {}
        per_tag_calls: dict[str, int] = {}
        for r in recs:
            per_tag_tokens[r["tag"]] = per_tag_tokens.get(r["tag"], 0) + r["input_tokens"]
            per_tag_calls[r["tag"]] = per_tag_calls.get(r["tag"], 0) + 1
        lat = [r["latency_s"] for r in recs]
        walls = [tags[t]["wall_beam_s"] for t in tags]
        mean_tok = statistics.mean(per_tag_tokens.values())
        mean_calls = statistics.mean(per_tag_calls.values())
        root_tok = [
            r["input_tokens"]
            for r in recs
            if r["parent"].startswith("`musicbrainz_tag` is a tag that MusicBrainz users applied to an artist, release or label. Which")
        ]
        greedy_calls_standalone = sum(1 if not tags[t]["greedy"]["path"] else 2 for t in tags)
        beam_calls = sum(per_tag_calls.values())
        full = {}
        for label, n in (
            ("tags_ge20_uses", vocab["tags_with_20plus_uses"]),
            ("tags_ge5_uses", vocab["tags_with_5plus_uses"]),
            ("all_distinct", vocab["distinct_tags_in_scope"]),
        ):
            full[label] = {
                "tags": n,
                "requests": round(n * mean_calls),
                "input_tokens": round(n * mean_tok),
                "usd": round(n * mean_tok / 1e6 * JEV_USD_PER_MTOK_IN, 2),
                "sequential_hours_at_sample_wall": round(n * statistics.mean(walls) / 3600, 2),
                "hours_at_8_parallel_tags": round(n * statistics.mean(walls) / 3600 / 8, 2),
            }
        summary["jev_cost"] = {
            "model": jev["model"],
            "price_usd_per_mtok_input": JEV_USD_PER_MTOK_IN,
            "sample_requests_cold": beam_calls,
            "sample_input_tokens": sum(per_tag_tokens.values()),
            "sample_usd": round(sum(per_tag_tokens.values()) / 1e6 * JEV_USD_PER_MTOK_IN, 4),
            "mean_requests_per_tag_beam": round(mean_calls, 2),
            "mean_input_tokens_per_tag_beam": round(mean_tok),
            "mean_input_tokens_root_request": round(statistics.mean(root_tok)) if root_tok else None,
            "latency_per_request_s": {"p50": round(pct(lat, 0.5), 3), "p95": round(pct(lat, 0.95), 3), "max": round(max(lat), 3)},
            "wall_per_tag_beam_s": {"p50": round(pct(walls, 0.5), 3), "p95": round(pct(walls, 0.95), 3)},
            "sample_wall_s": jev["wall_s"],
            "cache": {
                "greedy_requests_if_run_alone": greedy_calls_standalone,
                "greedy_requests_after_beam": 0,
                "beam_plus_greedy_without_cache": beam_calls + greedy_calls_standalone,
                "beam_plus_greedy_with_cache": beam_calls,
                "saved_share": round(greedy_calls_standalone / (beam_calls + greedy_calls_standalone), 3),
                "replay_requests": 0,
            },
            "full_run_estimates": full,
        }
    fc = load("flat_claude.json")
    if fc:
        recs = list(json.loads((DATA / "captures" / "claude_cache.json").read_text(encoding="utf-8")).values())
        cost = [
            (
                r["input_tokens"] * OPUS_IN
                + r["cache_creation_input_tokens"] * OPUS_CACHE_WRITE
                + r["cache_read_input_tokens"] * OPUS_CACHE_READ
                + r["output_tokens"] * OPUS_OUT
            )
            / 1e6
            for r in recs
        ]
        lat = [r["latency_s"] for r in recs]
        mean_cost = statistics.mean(cost)
        summary["claude_cost"] = {
            "model": fc["model"],
            "effort": fc["effort"],
            "requests": len(recs),
            "sample_usd": round(sum(cost), 4),
            "mean_usd_per_tag": round(mean_cost, 5),
            "mean_output_tokens": round(statistics.mean(r["output_tokens"] for r in recs)),
            "cache_read_tokens_total": sum(r["cache_read_input_tokens"] for r in recs),
            "cache_write_tokens_total": sum(r["cache_creation_input_tokens"] for r in recs),
            "latency_per_request_s": {"p50": round(pct(lat, 0.5), 3), "p95": round(pct(lat, 0.95), 3)},
            "full_run_usd_tags_ge5_uses": round(mean_cost * vocab["tags_with_5plus_uses"], 2),
            "full_run_usd_all_distinct": round(mean_cost * vocab["distinct_tags_in_scope"], 2),
            "stats": fc["stats"],
        }
    # Cascade: normalized string match first; on its misses, Jev beam K=3 and flat Claude
    # each propose, and their agreement (not separation) is the confidence signal.
    preds = predictions()
    if {"string_normalized", "jev_beam3", "flat_claude"} <= preds.keys():
        sm, jb, fc = preds["string_normalized"], preds["jev_beam3"], preds["flat_claude"]
        by_tag = {r["tag"]: r for r in rows}

        def bucket(t: str) -> str:
            if sm.get(t, ()) != ():
                return "string"
            if jb.get(t) == fc.get(t):
                return "agree_none" if jb.get(t) == () else "agree_genre"
            return "disagree"

        def answer(t: str) -> tuple[str, ...]:
            return sm[t] if bucket(t) == "string" else jb.get(t, ("__MISSING__",))

        cascade: dict = {}
        for b in ("string", "agree_genre", "agree_none", "disagree"):
            ts = [t for t in labels if bucket(t) == b]
            entry = {
                "n": len(ts),
                "jev_exact": round(sum(score(jb.get(t, ()), labels[t])["exact"] for t in ts) / len(ts), 3) if ts else None,
                "claude_exact": round(sum(score(fc.get(t, ()), labels[t])["exact"] for t in ts) / len(ts), 3) if ts else None,
            }
            for scope, strata in (("tags_ge20_uses", ("head", "mid")), ("tags_ge5_uses", tuple(sizes))):
                entry[f"est_{scope}"] = round(sum(sizes[s] * sum(by_tag[t]["stratum"] == s for t in ts) / n_by[s] for s in strata if s in sizes))
            if b == "string":
                entry["precision"] = round(sum(score(sm[t], labels[t])["exact"] for t in ts) / len(ts), 3) if ts else None
            if b == "agree_none":
                entry["missed_genre"] = sorted(t for t in ts if () not in labels[t])
            cascade[b] = entry
        auto = [t for t in labels if bucket(t) in ("string", "agree_genre", "agree_none")]
        cascade["auto_precision"] = round(sum(score(answer(t), labels[t])["exact"] for t in auto) / len(auto), 3)
        cascade["auto_use_share"] = round(sum(by_tag[t]["uses"] for t in auto) / sum(by_tag[t]["uses"] for t in labels), 3)
        cascade["review_agree_genre_plus_disagree"] = {
            scope: cascade["agree_genre"][f"est_{scope}"] + cascade["disagree"][f"est_{scope}"] for scope in ("tags_ge20_uses", "tags_ge5_uses")
        }
        summary["cascade"] = cascade
    RES.mkdir(exist_ok=True)
    (RES / "summary.json").write_text(json.dumps(summary, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    print("| method | exact | genre-level | genre-like exact | junk exact | head | mid | tail | stratum-wtd | use-wtd |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for name, m in methods.items():
        s = m["by_stratum"]
        print(
            f"| {name} | {m['exact']} | {m['genre_level']} | {m['genre_like_exact']} | {m['junk_exact']} | {s['head']} | {s['mid']} | {s['tail']} | {m['stratum_weighted_exact']} | {m['use_weighted_exact']} |"
        )
    print(json.dumps({k: v for k, v in summary.items() if k not in ("methods",)}, indent=1))


if __name__ == "__main__":
    main()
