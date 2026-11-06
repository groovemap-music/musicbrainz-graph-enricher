"""The published relationship-type contract is the enricher's own map, exactly.

catalog-api promotes ``contracts/projections/v1/relationship-types.json`` and fails its build when its typed
queries omit a type in it. That only protects the consumer if this file cannot drift from what the enricher
writes, so a changed ``MB_RELATIONSHIP_MAP`` fails here until the file and its record are regenerated.
"""

import json
from hashlib import sha256
from pathlib import Path

from brainzgraphinator._projections import MB_RELATIONSHIP_MAP


CONTRACT_DIR = Path(__file__).resolve().parents[1] / "contracts" / "projections" / "v1"
CONTRACT = CONTRACT_DIR / "relationship-types.json"
RECORD = CONTRACT_DIR / "source.json"
REGENERATE = "regenerate contracts/projections/v1 from MB_RELATIONSHIP_MAP (see the contract's description) and update source.json"


def test_the_contract_lists_exactly_the_maps_types() -> None:
    published = json.loads(CONTRACT.read_text())
    assert published["relationship_types"] == MB_RELATIONSHIP_MAP, REGENERATE


def test_the_edge_types_are_the_maps_distinct_values() -> None:
    published = json.loads(CONTRACT.read_text())
    assert published["edge_types"] == sorted(set(MB_RELATIONSHIP_MAP.values())), REGENERATE


def test_the_contract_is_identified_and_versioned() -> None:
    published = json.loads(CONTRACT.read_text())
    assert published["contract"] == "groovemap.musicbrainz-graph-enricher.relationship-types"
    assert published["version"] == 1
    assert published["store"] == "neo4j"


def test_the_record_matches_the_published_bytes() -> None:
    record = json.loads(RECORD.read_text())
    assert record["contract_sha256"] == sha256(CONTRACT.read_bytes()).hexdigest(), "source.json does not describe this file"
    assert record["producer_repository"] == "https://github.com/groovemap-music/musicbrainz-graph-enricher"
    assert record["producer_source"] == "brainzgraphinator/_projections.py MB_RELATIONSHIP_MAP"


def test_the_contract_file_is_stable_to_reserialize() -> None:
    text = CONTRACT.read_text()
    assert text == json.dumps(json.loads(text), indent=2, ensure_ascii=False) + "\n"
