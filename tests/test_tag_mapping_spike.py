"""Synthetic algorithm checks; no music-tag labels or provider results are claimed."""

import hashlib
import importlib.util
import json
from pathlib import Path

import pytest


spec = importlib.util.spec_from_file_location("tag_spike", Path(__file__).parents[1] / "scripts/spike_tag_mapping.py")
assert spec and spec.loader
spike = importlib.util.module_from_spec(spec)
spec.loader.exec_module(spike)


def capture(cache, tag, tree, path, probabilities):
    key = spike.request_key(tag, spike.children(tree, path), "synthetic", path)
    cache[key] = {"model": spike.MODEL, "probabilities": probabilities}


def test_beam_recovers_from_ambiguous_root_and_preserves_shared_style_paths():
    tree = {"A": ["shared"], "B": ["shared"]}
    cache = {}
    capture(cache, "synthetic tag", tree, (), {"A": 0.6, "B": 0.39, spike.NONE: 0.01})
    capture(cache, "synthetic tag", tree, ("A",), {"shared": 0.51, spike.GENRE_ONLY: 0.49})
    capture(cache, "synthetic tag", tree, ("B",), {"shared": 0.99, spike.GENRE_ONLY: 0.01})
    greedy = spike.traverse("synthetic tag", tree, cache, "synthetic", 1)
    beam = spike.traverse("synthetic tag", tree, cache, "synthetic", 3)
    assert greedy[0]["path"] == ["A", "shared"]
    assert beam[0]["path"] == ["B", "shared"]
    assert beam[0]["score"] == pytest.approx((0.39 * 0.99) ** 0.5)


def test_singleton_genre_only_does_not_inflate_confidence_or_require_call():
    tree = {"A": []}
    cache = {}
    capture(cache, "synthetic tag", tree, (), {"A": 0.4, spike.NONE: 0.6})
    beam = spike.traverse("synthetic tag", tree, cache, "synthetic", 3)
    assert beam == [{"path": [spike.NONE], "score": 0.6}, {"path": ["A", spike.GENRE_ONLY], "score": 0.4}]
    assert spike.leaf(beam[0]["path"]) == []
    assert spike.leaf(beam[1]["path"]) == ["A"]


@pytest.mark.parametrize("probabilities", [{"A": 1.0}, {"A": 0.8, spike.NONE: 0.8}, {"A": float("nan"), spike.NONE: 0.1}])
def test_incomplete_or_invalid_evidence_fails_closed(probabilities):
    tree = {"A": []}
    cache = {}
    capture(cache, "synthetic", tree, (), probabilities)
    with pytest.raises(ValueError):
        spike.traverse("synthetic", tree, cache, "synthetic", 3)


def test_missing_capture_and_wrong_model_cannot_be_reported_as_measurements():
    tree = {"A": []}
    with pytest.raises(KeyError):
        spike.traverse("synthetic", tree, {}, "synthetic", 3)
    cache = {}
    capture(cache, "synthetic", tree, (), {"A": 1.0, spike.NONE: 0.0})
    next(iter(cache.values()))["model"] = "jev-1.13.0"
    with pytest.raises(ValueError, match=r"Jev 1\.12"):
        spike.traverse("synthetic", tree, cache, "synthetic", 3)
    assert spike.request_key("x", ["a", "b"], "v1") != spike.request_key("x", ["b", "a"], "v1")
    assert spike.request_key("x", ["a"], "v1") != spike.request_key("x", ["a"], "v2")


def test_inventory_distinguishes_missing_fields_from_empty_tags_and_votes(tmp_path):
    corpus = tmp_path / "synthetic.jsonl"
    records = [
        {},
        {"tags": []},
        {"tags": [{"name": "example", "count": 7}, {"name": "other", "count": 2}]},
        {"tags": [{"name": "example", "count": 3}]},
    ]
    corpus.write_text("\n".join(json.dumps(row) for row in records) + "\n")
    result = spike.inventory(corpus, 4)
    assert result["truncated"] is False
    assert result["records_with_tags_field"] == 3
    assert result["tagged_records"] == 2
    assert result["distinct_tags"] == 2
    assert result["tag_occurrences"] == 3
    assert result["nonnegative_vote_sum"] == 12
    assert result["genre_like_share"] is None
    assert "example" not in json.dumps(result)
    assert spike.inventory(corpus, 2)["truncated"] is True


def test_unreviewed_labels_cannot_enter_accuracy_report():
    bundle = {"taxonomy": {"genres": {"A": []}, "provenance": "synthetic only"}, "labels": [{"tag": "synthetic", "accepted_paths": [["A"]]}]}
    with pytest.raises(ValueError, match="human label provenance"):
        spike.evaluate(bundle)


def test_replay_uses_common_denominator_and_leaves_unmeasured_claims_empty():
    taxonomy = {"genres": {"A": []}, "provenance": "synthetic only"}
    revision = hashlib.sha256(json.dumps(taxonomy, sort_keys=True).encode()).hexdigest()
    key = spike.request_key("synthetic", ["A", spike.NONE], revision)
    bundle = {
        "taxonomy": taxonomy,
        "labels": [
            {"tag": "synthetic", "accepted_paths": [[]], "reviewed_by": "test fixture", "reviewed_at": "synthetic", "source": "synthetic only"}
        ],
        "captures": {key: {"model": spike.MODEL, "probabilities": {"A": 0.4, spike.NONE: 0.6}}},
        "flat_claude": {"synthetic": {"model": "claude-synthetic", "capture_source": "test fixture", "path": ["A"]}},
    }
    result = spike.evaluate(bundle)
    assert result["accuracy"] == {"greedy": 1.0, "beam_k3": 1.0, "flat_claude": 0.0, "string_match": 0.0}
    assert result["threshold_sweep"][0] == {"threshold": 1.0, "accepted": 1, "accepted_errors": 0, "review": 0}
    assert result["threshold_sweep"][-1]["review"] == 1
    assert result["chosen_auto_accept_threshold"] is None
    assert result["provider_cost_latency_rate"] is None
    assert result["verdict"] is None
