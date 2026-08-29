"""Publication identity and documentation regressions."""

from pathlib import Path

import brainzgraphinator
import brainzgraphinator.brainzgraphinator as service


ROOT = Path(__file__).parent.parent
PUBLIC_DOCS = (
    ROOT / "README.md",
    ROOT / "brainzgraphinator/README.md",
    ROOT / "docs/README.md",
    ROOT / "docs/consumer-cancellation.md",
    ROOT / "docs/database-resilience.md",
    ROOT / "docs/file-completion-tracking.md",
    ROOT / "docs/graph-enrichment.md",
    ROOT / "docs/musicbrainz-sync.md",
)
ACTIVE_SOURCE_TESTS = (
    ROOT / "brainzgraphinator/brainzgraphinator.py",
    *(ROOT / "tests").glob("*.py"),
)


def test_runtime_identity_uses_repository_name() -> None:
    assert service.SERVICE_NAME == "musicbrainz-graph-enricher"
    assert "musicbrainz-graph-enricher" in (brainzgraphinator.__doc__ or "")
    assert service.get_health_data()["service"] == service.SERVICE_NAME
    assert service.SERVICE_NAME in service.STARTUP_BANNER


def test_active_documentation_uses_groovemap_identity() -> None:
    for path in PUBLIC_DOCS:
        text = path.read_text()
        assert "Discogsography" not in text
        assert "discogsography" not in text


def test_active_source_and_regressions_do_not_use_migration_issue_names() -> None:
    migration_issue_prefix = "discogs" + "ography-"
    for path in ACTIVE_SOURCE_TESTS:
        assert migration_issue_prefix not in path.read_text().lower()


def test_readme_covers_the_service_operating_contract() -> None:
    readme = (ROOT / "README.md").read_text()
    for required in (
        "## Data flow",
        "```mermaid",
        "## Processing and shutdown",
        "Transient",
        "## Configuration",
        "NEO4J_PORT",
        "Only the credential variables",
        "malformed JSON and other processing errors follow the generic requeue",
        "## Development",
        "## Contracts and compatibility",
        "brainzgraphinator",
    ):
        assert required in readme


def test_legacy_name_is_explicitly_limited_to_compatibility() -> None:
    assert service.WIRE_CONSUMER_NAME == "brainzgraphinator"
    compatibility_docs = "\n".join(
        (ROOT / path).read_text()
        for path in (
            "README.md",
            "brainzgraphinator/README.md",
            "docs/musicbrainz-sync.md",
        )
    )
    assert "wire-compatibility identifier" in compatibility_docs
    assert "not the public service identity" in compatibility_docs
