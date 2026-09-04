"""Regression tests for brainzgraphinator.queue_names.

ADR 0005 (docs/adr/0005-source-owned-catalog-ingestion.md in the
groovemap-music/design repository) freezes this service's exchange, queue,
and dead-letter identifiers across the catalog-ingestion split. These are
the exact strings the retired combined ``catalog_contract`` binding produced
for this service before the musicbrainz-only v1 contract was promoted.
"""

import json
from pathlib import Path

from brainzgraphinator.queue_names import (
    dead_letter_exchange_name,
    dead_letter_queue_name,
    exchange_name,
    queue_name,
)


ROOT = Path(__file__).parent.parent
CONSUMER = "brainzgraphinator"
ENTITIES = ["artists", "labels", "release-groups", "releases"]

# Frozen by ADR 0005: exchanges are "{exchange_prefix}-{entity}" and queues are
# "{exchange_prefix}-{consumer}-{entity}", with the "groovemap-musicbrainz" prefix
# and "brainzgraphinator" consumer this service has always used.
FROZEN_EXCHANGES = {
    "artists": "groovemap-musicbrainz-artists",
    "labels": "groovemap-musicbrainz-labels",
    "release-groups": "groovemap-musicbrainz-release-groups",
    "releases": "groovemap-musicbrainz-releases",
}
FROZEN_QUEUES = {
    "artists": "groovemap-musicbrainz-brainzgraphinator-artists",
    "labels": "groovemap-musicbrainz-brainzgraphinator-labels",
    "release-groups": "groovemap-musicbrainz-brainzgraphinator-release-groups",
    "releases": "groovemap-musicbrainz-brainzgraphinator-releases",
}


def test_adapter_matches_the_frozen_runtime_identifiers() -> None:
    for entity in ENTITIES:
        assert exchange_name(entity) == FROZEN_EXCHANGES[entity]
        queue = queue_name(CONSUMER, entity)
        assert queue == FROZEN_QUEUES[entity]
        assert dead_letter_exchange_name(CONSUMER, entity) == f"{queue}.dlx"
        assert dead_letter_queue_name(CONSUMER, entity) == f"{queue}.dlq"


def test_adapter_matches_the_promoted_contracts_runtime_identifiers() -> None:
    contract = json.loads((ROOT / "contracts/catalog-events/v1/contract.json").read_text())
    runtime_identifiers = contract["runtime_identifiers"]
    for entity in ENTITIES:
        assert exchange_name(entity) == runtime_identifiers["exchanges"][entity]
        queue_spec = runtime_identifiers["queues"][CONSUMER][entity]
        assert queue_name(CONSUMER, entity) == queue_spec["name"]
        assert dead_letter_exchange_name(CONSUMER, entity) == queue_spec["dead_letter_exchange"]
        assert dead_letter_queue_name(CONSUMER, entity) == queue_spec["dead_letter_queue"]
