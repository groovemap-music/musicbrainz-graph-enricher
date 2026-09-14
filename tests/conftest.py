"""Pytest configuration for brainzgraphinator tests."""

from unittest.mock import patch

import pytest

from tests.neo4j_doubles import neo4j_driver, neo4j_transaction


# Every standard OpenTelemetry variable that changes what the SDK records or exports. A test
# run must not inherit the ambient OpenTelemetry configuration -- OTEL_SDK_DISABLED=true in
# particular turns every SDK meter into a no-op, which would make in-memory-reader assertions
# fail with an empty collection and no error anywhere. Mirrors python-libraries' own
# tests/conftest.py isolation fixture.
OTEL_ENVIRONMENT = (
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    "OTEL_METRICS_EXEMPLAR_FILTER",
    "OTEL_METRICS_EXPORTER",
    "OTEL_METRIC_EXPORT_INTERVAL",
    "OTEL_PROPAGATORS",
    "OTEL_RESOURCE_ATTRIBUTES",
    "OTEL_SDK_DISABLED",
    "OTEL_SERVICE_NAME",
    "OTEL_TRACES_EXPORTER",
    "OTEL_TRACES_SAMPLER",
    "OTEL_TRACES_SAMPLER_ARG",
)


@pytest.fixture(autouse=True)
def isolated_otel_environment(monkeypatch: pytest.MonkeyPatch):
    """Run every test against a known-empty OpenTelemetry configuration."""
    for name in OTEL_ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)
    yield


@pytest.fixture(autouse=True)
def disable_batch_mode():
    """Disable batch mode for all brainzgraphinator tests."""
    with patch("brainzgraphinator.brainzgraphinator.BATCH_MODE", False):
        yield


@pytest.fixture
def mock_neo4j_driver():
    """Create an interface-faithful resilient Neo4j driver."""
    return neo4j_driver()


@pytest.fixture
def mock_tx():
    """Create an interface-faithful async transaction and result."""
    return neo4j_transaction()


@pytest.fixture
def sample_artist_record():
    """Sample MusicBrainz artist record for testing."""
    return {
        "id": "b10bbbfc-cf9e-42e0-be17-e2c3e1d2600d",
        "mbid": "b10bbbfc-cf9e-42e0-be17-e2c3e1d2600d",
        "discogs_artist_id": 12345,
        "type": "Person",
        "gender": "Male",
        "begin_date": "1947-01-08",
        "end_date": "2016-01-10",
        "area": "London, England",
        "begin_area": "Brixton, London",
        "end_area": "New York City",
        "disambiguation": "David Robert Jones",
        "relations": [
            {
                "type": "member of band",
                "target_discogs_artist_id": 67890,
            },
            {
                "type": "collaboration",
                "target_discogs_artist_id": 11111,
            },
        ],
    }


@pytest.fixture
def sample_label_record():
    """Sample MusicBrainz label record for testing."""
    return {
        "id": "c595c289-47ce-4fba-b999-b87503e8cb71",
        "mbid": "c595c289-47ce-4fba-b999-b87503e8cb71",
        "discogs_label_id": 54321,
        "type": "Original Production",
        "label_code": "1234",
        "begin_date": "1958",
        "end_date": None,
        "area": "New York",
    }


@pytest.fixture
def sample_release_record():
    """Sample MusicBrainz release record for testing."""
    return {
        "id": "a5d5abbc-fb46-427c-9e5f-8da2f0bdbb18",
        "mbid": "a5d5abbc-fb46-427c-9e5f-8da2f0bdbb18",
        "discogs_release_id": 99999,
        "barcode": "724384952051",
        "status": "Official",
    }


@pytest.fixture
def sample_release_group_record():
    """Sample MusicBrainz release-group record for testing."""
    return {
        "id": "1dc4c347-a1db-32aa-b14f-bc9cc507b843",
        "mbid": "1dc4c347-a1db-32aa-b14f-bc9cc507b843",
        "discogs_master_id": 23853,
        "type": "Album",
        "secondary_types": ["Compilation"],
        "first_release_date": "1969-09-26",
        "disambiguation": "",
    }
