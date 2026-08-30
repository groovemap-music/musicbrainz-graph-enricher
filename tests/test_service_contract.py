"""Import and promoted catalog-contract smoke tests."""

import brainzgraphinator.brainzgraphinator as service
from brainzgraphinator.catalog_contract import AMQP_EXCHANGE_TYPE, MUSICBRAINZ_DATA_TYPES, MUSICBRAINZ_EXCHANGE_PREFIX


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
