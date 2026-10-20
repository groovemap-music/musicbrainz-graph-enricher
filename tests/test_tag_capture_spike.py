"""Fake-transport checks for the spike tooling. No network, no provider, no SDK; all data is synthetic fixtures."""

import importlib.util
import json
import sys
from pathlib import Path

import pytest


SCRIPTS = Path(__file__).parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))


def load(name):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


base = load("spike_tag_mapping")
cap = load("spike_tag_capture")
export = load("spike_taxonomy_export")

SECRET = "synthetic-secret-key"
ENV = {cap.KEY_ENV: SECRET}
TAXONOMY = {"genres": {"A": ["shared"], "B": ["shared"]}, "provenance": "SYNTHETIC FIXTURE, not evidence"}


class FakeTransport:
    def __init__(self, script=None, resolved=base.MODEL, returned=base.MODEL):
        self.script, self.resolved, self.returned = list(script or []), resolved, returned
        self.calls = []

    def verify_model(self, _model, api_key):
        assert api_key == SECRET
        return self.resolved

    def classify(self, request, _api_key):
        self.calls.append(request)
        if self.script:
            step = self.script.pop(0)
            if isinstance(step, Exception):
                raise step
        options = request["options"]
        probabilities = {option: 1 / len(options) for option in options}
        return {"model": self.returned, "probabilities": probabilities, "usage": {"input_tokens": 100, "output_tokens": 10}}


class Clock:
    def __init__(self):
        self.now, self.slept = 0.0, []

    def __call__(self):
        self.now += 0.5
        return self.now

    def sleep(self, seconds):
        self.slept.append(seconds)


def make(transport, **kwargs):
    clock = Clock()
    kwargs.setdefault("max_requests", 50)
    return cap.Capturer(transport, TAXONOMY, env=ENV, sleep=clock.sleep, clock=clock, **kwargs), clock


def test_refuses_without_credential_or_pinned_model():
    with pytest.raises(cap.CaptureRefused, match=cap.KEY_ENV):
        cap.Capturer(FakeTransport(), TAXONOMY, env={}, max_requests=1)
    with pytest.raises(cap.CaptureRefused, match="never substituting"):
        make(FakeTransport(resolved="jev-1.13"))
    with pytest.raises(cap.CaptureRefused, match="never substituting"):
        make(FakeTransport(resolved="jev-latest"))


def test_returned_model_mismatch_aborts_and_is_not_cached():
    capturer, _ = make(FakeTransport(returned="jev-1.13"))
    with pytest.raises(cap.CaptureRefused):
        capturer.run(["synthetic"])
    assert capturer.cache == {}
    assert capturer.failures[0]["kind"] == "model_mismatch"


def test_output_feeds_replay_and_never_contains_the_credential():
    capturer, _ = make(FakeTransport())
    bundle = capturer.run(["one", "one", "two"])
    assert bundle["tags_completed"] == ["one", "two"]
    labels = [
        {"tag": t, "accepted_paths": [[]], "reviewed_by": "fixture", "reviewed_at": "fixture", "source": "fixture"} for t in bundle["tags_completed"]
    ]
    flat = {t: {"model": "claude-fixture", "capture_source": "fixture", "path": []} for t in bundle["tags_completed"]}
    result = base.evaluate({**bundle, "labels": labels, "flat_claude": flat})
    assert result["sample_size"] == 2
    assert SECRET not in json.dumps(bundle)
    assert {m["attempts"] for m in bundle["measurements"].values()} == {1}


def test_rate_limit_retry_after_is_honoured_and_secret_redacted_from_errors():
    transport = FakeTransport([cap.RateLimited(7.0), cap.RateLimited(None)])
    capturer, clock = make(transport, base_backoff=2.0)
    capturer.run(["one"])
    assert clock.slept[:2] == [7.0, 4.0]
    assert capturer.rate_limit_responses == 2
    failing, _ = make(FakeTransport([RuntimeError(f"boom {SECRET}")]))
    bundle = failing.run(["one"])
    assert bundle["tags_failed"] == ["one"]
    assert SECRET not in json.dumps(bundle)


def test_retry_after_beyond_bound_records_failure_without_sleeping_through_it():
    capturer, clock = make(FakeTransport([cap.RateLimited(3600)]), max_wait=30)
    bundle = capturer.run(["one"])
    assert bundle["tags_failed"] == ["one"]
    assert clock.slept == []
    assert bundle["failures"][0]["kind"] == "rate_limited"


def test_request_budget_is_hard_and_remaining_tags_are_reported_skipped():
    capturer, _ = make(FakeTransport(), max_requests=3)
    bundle = capturer.run(["one", "two", "three"])
    assert capturer.requests == 3
    assert bundle["tags_skipped"]
    assert set(bundle["tags_completed"]) | set(bundle["tags_skipped"]) == {"one", "two", "three"}


def test_invalid_distribution_is_a_failure_not_a_capture():
    transport = FakeTransport()
    transport.classify = lambda _request, _key: {"model": base.MODEL, "probabilities": {"A": 1.0}, "usage": None}
    capturer, _ = make(transport)
    bundle = capturer.run(["one"])
    assert bundle["failures"][0]["kind"] == "invalid_distribution"
    assert capturer.cache == {}


def test_cache_replay_counts_latency_and_resumes_from_prior_bundle():
    cold, _ = make(FakeTransport())
    first = cold.run(["one"])
    transport = FakeTransport()
    warm, _ = make(transport, cache=first["captures"], measurements=first["measurements"])
    second = warm.run(["one"])
    assert transport.calls == []
    assert second["stats"]["cold"]["requests"] == 0
    assert second["stats"]["replay"]["hits"] == first["stats"]["cold"]["requests"]
    assert second["stats"]["replay"]["tokens_avoided"]["input_tokens"] == 100 * first["stats"]["cold"]["requests"]
    assert first["stats"]["cold"]["latency_seconds"] > 0


def test_cache_identity_includes_parent_so_shared_styles_do_not_collide():
    capturer, _ = make(FakeTransport())
    capturer.run(["one"])
    parents = {tuple(call["parent"]) for call in capturer.transport.calls}
    assert ("A",) in parents
    assert ("B",) in parents


def test_extrapolator_uses_measured_inputs_and_actual_vocabulary():
    capturer, _ = make(FakeTransport())
    bundle = capturer.run(["one", "two"])
    result = cap.extrapolate(bundle, 1000)
    per_tag = len(bundle["measurements"]) / 2
    assert result["projected_requests"] == pytest.approx(per_tag * 1000)
    assert result["projected_cost"] is None
    assert result["projected_tokens"]["input"] == pytest.approx(100 * per_tag * 1000)
    rates = {"input_per_million": 2.0, "output_per_million": 4.0, "currency": "USD", "source": "SYNTHETIC FIXTURE"}
    priced = cap.extrapolate(bundle, 1000, rates)
    assert priced["projected_cost"]["amount"] == pytest.approx((100 * 2.0 + 10 * 4.0) * per_tag * 1000 / 1_000_000)


@pytest.mark.parametrize("size", [None, 0, -5, True, 1.5])
def test_extrapolator_refuses_without_actual_vocabulary_size(size):
    capturer, _ = make(FakeTransport())
    with pytest.raises(ValueError, match="vocabulary"):
        cap.extrapolate(capturer.run(["one"]), size)


def test_extrapolator_refuses_without_measurements_or_usage_or_rates_source():
    with pytest.raises(ValueError, match="refusing"):
        cap.extrapolate({"measurements": {}, "tags_completed": []}, 100)
    capturer, _ = make(FakeTransport())
    bundle = capturer.run(["one"])
    next(iter(bundle["measurements"].values()))["usage"] = None
    with pytest.raises(ValueError, match="usage"):
        cap.extrapolate(bundle, 100)
    good, _ = make(FakeTransport())
    with pytest.raises(ValueError, match="authoritative source"):
        cap.extrapolate(good.run(["one"]), 100, {"input_per_million": 1, "output_per_million": 1, "currency": "USD"})


def test_cli_capture_refuses_without_credential(tmp_path, monkeypatch):
    monkeypatch.delenv(cap.KEY_ENV, raising=False)
    (tmp_path / "t.json").write_text(json.dumps(TAXONOMY))
    (tmp_path / "tags.txt").write_text("one\n")
    module = tmp_path / "fake_factory_mod.py"
    module.write_text("class T:\n    def verify_model(self, m, k):\n        return m\n\ndef make():\n    return T()\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    argv = [
        "x",
        "capture",
        str(tmp_path / "t.json"),
        str(tmp_path / "tags.txt"),
        str(tmp_path / "out.json"),
        "--transport-factory",
        "fake_factory_mod:make",
        "--max-requests",
        "5",
    ]
    monkeypatch.setattr(sys, "argv", argv)
    with pytest.raises(SystemExit) as raised:
        cap.main()
    assert cap.KEY_ENV in str(raised.value)
    assert not (tmp_path / "out.json").exists()


FIXTURE_PROVENANCE = {"source": "SYNTHETIC FIXTURE", "snapshot_id": "none", "exported_at": "2000-01-01", "synthetic": True}


def test_taxonomy_export_keeps_sources_separate_and_reports_differences():
    observed = [{"genre": "A", "style": "s1", "count": 5}, {"genre": "B", "style": "s1", "count": 2}, {"genre": "A", "style": "s2", "count": 1}]
    part_of = [{"genre": "A", "style": "s1"}, {"genre": "C", "style": "s9"}]
    result = export.build(FIXTURE_PROVENANCE, observed, part_of)
    comparison = result["comparison"]
    assert result["synthetic"] is True
    assert comparison["in_both"] == 1
    assert comparison["observed_missing_from_part_of"] == [{"genre": "A", "style": "s2", "count": 1}, {"genre": "B", "style": "s1", "count": 2}]
    assert comparison["part_of_not_observed"] == [{"genre": "C", "style": "s9"}]
    assert result["shared_styles"] == {"s1": ["A", "B"]}
    assert result["observed_pairs"][0]["count"] == 5
    assert {"genre": "C", "style": "s9"} not in [{k: p[k] for k in ("genre", "style")} for p in result["observed_pairs"]]


@pytest.mark.parametrize(
    "observed",
    [
        [{"genre": "A", "style": "s", "count": 0}],
        [{"genre": "A", "style": "s", "count": True}],
        [{"genre": "A", "style": "s", "count": 1}, {"genre": "A", "style": "s", "count": 1}],
        [{"genre": "__NONE__", "style": "s", "count": 1}],
        [{"genre": "", "style": "s", "count": 1}],
    ],
)
def test_taxonomy_export_rejects_bad_rows(observed):
    with pytest.raises(ValueError):
        export.build(FIXTURE_PROVENANCE, observed, [])


def test_taxonomy_export_requires_provenance_and_reads_files(tmp_path):
    with pytest.raises(ValueError, match="provenance"):
        export.build({"source": "x"}, [], [])
    path = tmp_path / "rows.jsonl"
    path.write_text('{"genre": "A", "style": "s"}\n\n{"genre": "B", "style": "t"}\n')
    assert len(export.read_rows(path)) == 2
    path.write_text("{oops\n")
    with pytest.raises(ValueError, match="invalid JSON"):
        export.read_rows(path)
