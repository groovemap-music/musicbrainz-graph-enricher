"""Capture adapter, request cache, and cost/latency extrapolator for gm-musicbrainz-graph-enricher-569.

Provider-agnostic and stdlib-only. The adapter talks to an injected transport and
never imports an SDK; no real transport ships here because the TypeSafe wire format
has not been verified against live documentation. Credentials come only from the
environment (`TYPESAFE_API_KEY`) and are never written to captures. Tests use a fake
in-process transport; synthetic output is a fixture, never acceptance evidence.

Transport contract (duck-typed):
  verify_model(model, api_key) -> str       exact model id the account resolves for `model`
  classify(request, api_key) -> dict         {"model", "probabilities": {option: p}, "usage": {"input_tokens", "output_tokens"}}
Transports raise RateLimited(retry_after) on a rate-limit response and TransientError
on a retryable failure; any other exception is recorded as a non-retryable failure.
"""

import argparse
import hashlib
import importlib
import json
import math
import os
import sys
import time
from pathlib import Path


sys.path.insert(0, str(Path(__file__).parent))
import spike_tag_mapping as base


KEY_ENV = "TYPESAFE_API_KEY"


class CaptureRefused(RuntimeError):
    """Run must not start or continue: missing credential, unverifiable or substituted model."""


class RateLimited(Exception):
    def __init__(self, retry_after=None):
        super().__init__("rate limited")
        self.retry_after = retry_after


class TransientError(Exception):
    """Retryable provider/network failure."""


class CaptureFailure(Exception):
    """One request failed for good; the tag is recorded as failed."""


class BudgetExhausted(Exception):
    """The bounded request budget is spent."""


def model_ok(model):
    return isinstance(model, str) and (model == base.MODEL or model.startswith(base.MODEL + "."))


def taxonomy_sha(taxonomy):
    """Same identity the replay command derives from the bundle's taxonomy block."""
    return hashlib.sha256(json.dumps(taxonomy, sort_keys=True).encode()).hexdigest()


def valid_distribution(probabilities, options):
    if not isinstance(probabilities, dict) or set(probabilities) != set(options):
        return False
    if any(isinstance(p, bool) or not isinstance(p, int | float) or not math.isfinite(p) or not 0 <= p <= 1 for p in probabilities.values()):
        return False
    return math.isclose(sum(probabilities.values()), 1.0, abs_tol=1e-6)


def valid_usage(usage):
    if not isinstance(usage, dict):
        return None
    values = [usage.get("input_tokens"), usage.get("output_tokens")]
    if any(isinstance(v, bool) or not isinstance(v, int) or v < 0 for v in values):
        return None
    return {"input_tokens": values[0], "output_tokens": values[1]}


class Capturer:
    """Bounded, cached, rate-limit-aware requester that emits the replay `captures` shape."""

    def __init__(
        self,
        transport,
        taxonomy,
        *,
        env=None,
        max_requests,
        max_attempts=4,
        max_wait=60.0,
        base_backoff=1.0,
        sleep=time.sleep,
        clock=time.monotonic,
        cache=None,
        measurements=None,
    ):
        if max_requests < 1 or max_attempts < 1:
            raise ValueError("positive request and attempt bounds are required")
        env = os.environ if env is None else env
        self._key = (env.get(KEY_ENV) or "").strip()
        if not self._key:
            raise CaptureRefused(f"{KEY_ENV} is not set; refusing to run")
        if not taxonomy.get("genres") or not taxonomy.get("provenance"):
            raise ValueError("taxonomy with genres and provenance is required")
        try:
            resolved = transport.verify_model(base.MODEL, self._key)
        except Exception as error:
            raise CaptureRefused(f"cannot verify {base.MODEL}: {self._redact(error)}") from None
        if not model_ok(resolved):
            raise CaptureRefused(f"{base.MODEL} resolved to {resolved!r}; never substituting an alias or newer model")
        self.transport = transport
        self.taxonomy = taxonomy
        self.sha = taxonomy_sha(taxonomy)
        self.max_requests, self.max_attempts, self.max_wait, self.base_backoff = max_requests, max_attempts, max_wait, base_backoff
        self._sleep, self._clock = sleep, clock
        self.cache = {} if cache is None else cache
        self.measurements = {} if measurements is None else measurements
        self.failures = []
        self.requests = 0  # every attempt spends budget
        self.rate_limit_responses = 0
        self.cold_latency = self.replay_latency = 0.0
        self.cold_calls = self.replay_hits = self.tokens_avoided_in = self.tokens_avoided_out = 0

    def _redact(self, value):
        return str(value).replace(self._key, "[redacted]")

    def _fail(self, tag, parent, key, kind, message, attempts):
        self.failures.append({"tag": tag, "parent": list(parent), "key": key, "kind": kind, "message": self._redact(message), "attempts": attempts})
        raise CaptureFailure(kind)

    def resolve(self, tag, options, parent, key):
        """Resolver handed to base.traverse; the cache key is the full request identity."""
        start = self._clock()
        if key in self.cache:
            self.replay_hits += 1
            usage = (self.measurements.get(key) or {}).get("usage") or {}
            self.tokens_avoided_in += usage.get("input_tokens", 0)
            self.tokens_avoided_out += usage.get("output_tokens", 0)
            capture = self.cache[key]
            self.replay_latency += self._clock() - start
            return capture
        request = {"model": base.MODEL, "tag": tag, "options": list(options), "parent": list(parent), "instructions": base.INSTRUCTIONS}
        attempts = 0
        while True:
            if self.requests >= self.max_requests:
                raise BudgetExhausted
            self.requests += 1
            attempts += 1
            began = self._clock()
            try:
                response = self.transport.classify(request, self._key)
            except RateLimited as limited:
                self.rate_limit_responses += 1
                retry_after = limited.retry_after
                if isinstance(retry_after, bool) or not isinstance(retry_after, int | float) or retry_after < 0:
                    retry_after = min(self.base_backoff * 2 ** (attempts - 1), self.max_wait)
                if retry_after > self.max_wait or attempts >= self.max_attempts:
                    self._fail(tag, parent, key, "rate_limited", f"retry-after {retry_after}s exceeds bounds", attempts)
                self._sleep(retry_after)  # honour the provider's actual signal
                continue
            except TransientError as error:
                if attempts >= self.max_attempts:
                    self._fail(tag, parent, key, "transient", error, attempts)
                self._sleep(min(self.base_backoff * 2 ** (attempts - 1), self.max_wait))
                continue
            except Exception as error:
                self._fail(tag, parent, key, "error", error, attempts)
            latency = self._clock() - began
            break
        model = response.get("model") if isinstance(response, dict) else None
        if not model_ok(model):
            self.failures.append(
                {"tag": tag, "parent": list(parent), "key": key, "kind": "model_mismatch", "message": f"returned {model!r}", "attempts": attempts}
            )
            raise CaptureRefused(f"provider returned {model!r}, not {base.MODEL}; aborting rather than substituting")
        if not valid_distribution(response.get("probabilities"), options):
            self._fail(tag, parent, key, "invalid_distribution", "incomplete or invalid child distribution", attempts)
        self.cold_calls += 1
        self.cold_latency += latency
        self.cache[key] = {"model": model, "probabilities": dict(response["probabilities"])}
        self.measurements[key] = {"latency_seconds": latency, "usage": valid_usage(response.get("usage")), "attempts": attempts}
        return self.cache[key]

    def run(self, tags):
        """Classify distinct tags once each; returns a bundle `replay` can consume once labels are added."""
        completed, failed, skipped = [], [], []
        pending = list(dict.fromkeys(tags))
        for position, tag in enumerate(pending):
            try:
                base.traverse(tag, self.taxonomy["genres"], self.cache, self.sha, 3, resolve=self.resolve)
                completed.append(tag)
            except CaptureFailure:
                failed.append(tag)
            except BudgetExhausted:
                skipped = pending[position:]
                break
        return {
            "taxonomy": self.taxonomy,
            "captures": self.cache,
            "measurements": self.measurements,
            "failures": self.failures,
            "tags_completed": completed,
            "tags_failed": failed,
            "tags_skipped": skipped,
            "stats": self.stats(),
            "model_required": base.MODEL,
        }

    def stats(self):
        return {
            "requests_spent": self.requests,
            "rate_limit_responses": self.rate_limit_responses,
            "cold": {"requests": self.cold_calls, "latency_seconds": self.cold_latency},
            "replay": {
                "hits": self.replay_hits,
                "latency_seconds": self.replay_latency,
                "tokens_avoided": {"input_tokens": self.tokens_avoided_in, "output_tokens": self.tokens_avoided_out},
            },
        }


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


def extrapolate(bundle, vocabulary_size, rates=None):
    """Project from measured usage and the actual distinct-vocabulary size; refuse otherwise."""
    if isinstance(vocabulary_size, bool) or not isinstance(vocabulary_size, int) or vocabulary_size < 1:
        raise ValueError("the actual distinct-vocabulary size (from a full inventory) is required")
    measurements = bundle.get("measurements") or {}
    completed = bundle.get("tags_completed") or []
    if not measurements or not completed:
        raise ValueError("no measured requests or completed tags; refusing to extrapolate")
    for entry in measurements.values():
        latency = entry.get("latency_seconds")
        if isinstance(latency, bool) or not isinstance(latency, int | float) or not math.isfinite(latency) or latency < 0 or not entry.get("usage"):
            raise ValueError("every measured request needs latency and token usage; refusing to extrapolate")
    requests = len(measurements)
    tokens_in = sum(m["usage"]["input_tokens"] for m in measurements.values())
    tokens_out = sum(m["usage"]["output_tokens"] for m in measurements.values())
    latencies = [m["latency_seconds"] for m in measurements.values()]
    per_tag = requests / len(completed)
    projected_requests = per_tag * vocabulary_size
    stats = bundle.get("stats") or {}
    attempts = sum(m.get("attempts", 1) for m in measurements.values())
    result = {
        "basis": {
            "completed_tags": len(completed),
            "failed_tags": len(bundle.get("tags_failed") or []),
            "measured_requests": requests,
            "vocabulary_size": vocabulary_size,
        },
        "requests_per_tag": per_tag,
        "mean_tokens_per_request": {"input": tokens_in / requests, "output": tokens_out / requests},
        "projected_requests": projected_requests,
        "projected_tokens": {"input": tokens_in / requests * projected_requests, "output": tokens_out / requests * projected_requests},
        "latency_seconds": {
            "mean": sum(latencies) / requests,
            "p50": percentile(latencies, 0.5),
            "p95": percentile(latencies, 0.95),
            "projected_sequential_total": sum(latencies) / requests * projected_requests,
        },
        "retry_attempts_per_request": attempts / requests,
        "rate_limit_responses_observed": stats.get("rate_limit_responses"),
        "cache": {"measured_cold": stats.get("cold"), "measured_replay": stats.get("replay")},
        "projected_cost": None,
        "assumptions": [
            "linear in distinct tags at the measured requests-per-tag; no concurrency or provider rate limit is assumed or inferred",
            "valid only to the degree the measured tags resemble the whole vocabulary",
            "token counts exclude any failed attempts that returned no usage",
        ],
    }
    if rates is not None:
        needed = ("input_per_million", "output_per_million", "currency", "source")
        if any(not rates.get(field) for field in needed[2:]) or any(
            isinstance(rates.get(f), bool) or not isinstance(rates.get(f), int | float) or rates[f] < 0 for f in needed[:2]
        ):
            raise ValueError("rates need input_per_million, output_per_million, currency, and an authoritative source")
        tokens = result["projected_tokens"]
        result["projected_cost"] = {
            "amount": (tokens["input"] * rates["input_per_million"] + tokens["output"] * rates["output_per_million"]) / 1_000_000,
            "currency": rates["currency"],
            "rate_source": rates["source"],
        }
    return result


def load_transport(spec):
    module, _, name = spec.partition(":")
    if not module or not name:
        raise ValueError("--transport-factory must be module:callable")
    return getattr(importlib.import_module(module), name)()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    cap = sub.add_parser("capture", help="classify distinct tags through a supplied transport (never a built-in one)")
    cap.add_argument("taxonomy", type=Path, help="JSON with genres and provenance")
    cap.add_argument("tags", type=Path, help="one distinct tag per line")
    cap.add_argument("output", type=Path)
    cap.add_argument("--transport-factory", required=True, help="module:callable returning a transport; none ships with this script")
    cap.add_argument("--max-requests", type=int, required=True)
    ext = sub.add_parser("extrapolate")
    ext.add_argument("capture", type=Path)
    ext.add_argument("--vocabulary-size", type=int, required=True)
    ext.add_argument("--rates", type=Path, help="JSON: input_per_million, output_per_million, currency, source")
    args = parser.parse_args()
    try:
        if args.command == "capture":
            tags = [line.strip() for line in args.tags.read_text(encoding="utf-8").splitlines() if line.strip()]
            capturer = Capturer(load_transport(args.transport_factory), json.loads(args.taxonomy.read_text()), max_requests=args.max_requests)
            args.output.write_text(json.dumps(capturer.run(tags), indent=2, allow_nan=False))
            return
        rates = json.loads(args.rates.read_text()) if args.rates else None
        print(json.dumps(extrapolate(json.loads(args.capture.read_text()), args.vocabulary_size, rates), indent=2, allow_nan=False))
    except (CaptureRefused, ValueError) as error:
        sys.exit(f"refused: {error}")


if __name__ == "__main__":
    main()
