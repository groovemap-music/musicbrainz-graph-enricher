"""Static contracts for immutable, fail-closed repository automation."""

import json
import re
from pathlib import Path


ROOT = Path(__file__).parent.parent
AUTOMATION_REVISION = "833cb464507678c38ab78bd4718ce697399463e9"
PYTHON_LIBRARIES_REVISION = "24704f5fd48d3ef4fff29398585e9924e225b0c5"


def test_reusable_workflows_are_immutably_pinned() -> None:
    expected = {
        "ci.yml": "reusable-ci.yml",
        "release.yml": "reusable-release.yml",
    }
    for name, reusable_name in expected.items():
        workflow = (ROOT / ".github" / "workflows" / name).read_text()
        refs = re.findall(
            rf"uses: groovemap-music/automation/\.github/workflows/{reusable_name}@([^\s]+)",
            workflow,
        )
        assert refs == [AUTOMATION_REVISION]
        assert "groovemap-music/.github/" not in workflow
        assert "secrets: inherit" not in workflow


def test_dependabot_pull_requests_run_the_ordinary_required_ci_graph() -> None:
    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text()

    assert "pull_request:" in workflow
    assert "schedule:" in workflow
    assert "workflow_dispatch:" in workflow
    jobs = workflow.split("jobs:\n", 1)[1]
    assert len(re.findall(r"^  [a-zA-Z0-9_-]+:\s*$", jobs, re.MULTILINE)) == 1
    assert "jobs:\n  required:" in workflow
    assert "github.actor" not in workflow.lower()
    assert "dependabot" not in workflow.lower()
    assert "fallback-command" not in workflow
    assert "if:" not in workflow.lower()

    for fragment in (
        "language: python",
        "setup-command: just setup",
        "check-command: just check",
        "coverage-command: just coverage",
        "audit-command: just audit",
        "license-command: just license-check",
        "secret-scan-command: just secret-scan",
        "package-command: just build",
        "install-command: just install-check",
        "image-command: just image",
        "integration-command: just test-integration",
        "coverage-files: coverage.xml",
        "upload-codecov: true",
        "CODECOV_TOKEN: ${{ secrets.CODECOV_TOKEN }}",
    ):
        assert fragment in workflow

    for marker in (
        "requires-private-library",
        "private-library-client-id",
        "private-library-revision",
        "private_library_private_key",
        "groovemap_ci_app_client_id",
        "groovemap_ci_app_private_key",
    ):
        assert marker not in workflow.lower()

    pyproject = (ROOT / "pyproject.toml").read_text()
    assert "https://github.com/groovemap-music/python-libraries.git" in pyproject
    assert PYTHON_LIBRARIES_REVISION in pyproject


def test_release_is_tag_only_attested_and_repository_named() -> None:
    workflow = (ROOT / ".github" / "workflows" / "release.yml").read_text()

    assert re.search(r'on:\s*\n  push:\s*\n    tags: \["v\*"\]', workflow)
    assert "workflow_dispatch:" not in workflow
    assert "schedule:" not in workflow
    assert "branches:" not in workflow
    assert "attestations: write" in workflow
    assert "id-token: write" in workflow
    assert "packages: write" in workflow
    assert "repository-name: musicbrainz-graph-enricher" in workflow
    assert "release-command: just release-dry-run" in workflow
    assert "publish-image: true" in workflow
    assert "prepare-image-command: just prepare-runtime-wheel" in workflow
    assert "latest" not in workflow.lower()
    for marker in (
        "requires-private-library",
        "private-library-client-id",
        "private-library-revision",
        "private_library_private_key",
        "groovemap_ci_app_client_id",
        "groovemap_ci_app_private_key",
    ):
        assert marker not in workflow.lower()


def test_public_library_cutover_is_documented_as_complete() -> None:
    documents = (
        ROOT / "README.md",
        ROOT / "docs" / "release-compliance.md",
    )
    for path in documents:
        text = path.read_text()
        normalized = " ".join(text.split())
        assert "**Public-library cutover: complete.**" in text
        assert "without private-package credentials" in normalized

    readme = documents[0].read_text()
    assert "public `groovemap-music/python-libraries` repository" in readme
    assert "While that dependency is private" not in readme
    assert "GitHub App provides short-lived read access" not in readme


def test_required_regression_suites_remain_in_the_full_gate() -> None:
    expected_tests = {
        "tests/test_shutdown_delivery_churn.py": (
            "test_shutdown_guard_leaves_repeated_deliveries_unsettled",
            "test_shutdown_cancels_every_consumer_before_connection_close",
        ),
        "tests/test_brainzgraphinator.py": (
            "test_on_data_message_neo4j_error_requeues",
            "test_driver_unavailable_is_transient",
        ),
    }
    for relative_path, test_names in expected_tests.items():
        source = (ROOT / relative_path).read_text()
        for test_name in test_names:
            assert f"def {test_name}(" in source


def test_real_neo4j_lane_is_pinned_disposable_and_credential_free() -> None:
    contract = json.loads((ROOT / "contracts" / "integration-testing" / "v1" / "contract.json").read_text())
    image = contract["database"]["image"]
    script = (ROOT / "scripts" / "test-integration.sh").read_text()
    justfile = (ROOT / "Justfile").read_text()

    assert image == "neo4j:2026-community@sha256:dbc377fb9cd8fe8dabc19d3041b197d5ca0ef8bae514cea175b8df265e5b7a76"
    assert image in script
    assert "--publish 127.0.0.1::7687" in script
    assert "trap cleanup EXIT" in script
    assert 'docker rm --force "${container}"' in script
    assert "NEO4J_INTEGRATION_PASSWORD" in script
    assert "test-integration:\n    bash scripts/test-integration.sh" in justfile
    assert 'pytest -m "not integration"' in justfile


def test_no_renovate_or_legacy_claude_workflow_exists() -> None:
    repository_paths = [path.relative_to(ROOT).as_posix().lower() for path in ROOT.rglob("*") if path.is_file()]
    assert not any("renovate" in path for path in repository_paths)
    assert not any(path.startswith(".github/workflows/claude") for path in repository_paths)
