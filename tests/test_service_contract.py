"""Import and promoted catalog-contract smoke tests."""

from pathlib import Path

import brainzgraphinator.brainzgraphinator as service
from brainzgraphinator.catalog_contract import AMQP_EXCHANGE_TYPE, MUSICBRAINZ_DATA_TYPES, MUSICBRAINZ_EXCHANGE_PREFIX


ROOT = Path(__file__).parent.parent


def test_service_import_exposes_entry_point() -> None:
    assert callable(service.main)


def test_public_service_identity_matches_repository() -> None:
    assert service.SERVICE_NAME == "musicbrainz-graph-enricher"
    assert service.SERVICE_NAME in service.STARTUP_BANNER


def test_legacy_consumer_identity_remains_a_wire_compatibility_boundary() -> None:
    assert service.WIRE_CONSUMER_NAME == "brainzgraphinator"


def test_catalog_contract_matches_musicbrainz_stream() -> None:
    assert MUSICBRAINZ_EXCHANGE_PREFIX == "groovemap-musicbrainz"
    assert AMQP_EXCHANGE_TYPE == "fanout"
    assert MUSICBRAINZ_DATA_TYPES == ["artists", "labels", "release-groups", "releases"]


def test_public_docs_exclude_private_planning_material() -> None:
    assert not (ROOT / "docs" / "extraction.md").exists()
    assert not (ROOT / "docs" / "superpowers").exists()
    docs = "\n".join(path.read_text() for path in [ROOT / "README.md", *(ROOT / "docs").glob("*.md")])
    assert "Python 3.14" in docs
    assert "discogsography" not in docs.casefold()


def test_release_and_history_docs_keep_remote_mutations_separately_approved() -> None:
    release = (ROOT / "docs" / "release-compliance.md").read_text()
    history = (ROOT / "docs" / "history-rewrite-gate.md").read_text()
    assert "Dependabot-authored pull requests run the same required" in release
    assert "explicit operator approval" in release
    assert "Explicit operator approval" in history
    assert "visibility, tags, releases, packages, and container publication" in history
