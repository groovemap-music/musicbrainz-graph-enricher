# Release compliance

The repository gate is credential-free and does not contact a deployment. `just check`
verifies formatting, linting, types, tests and coverage, promoted catalog and schema contracts,
immutable automation, package construction and installation, MIT metadata, complete Git and
worktree secret scans, and version consistency. `just audit` adds the current network-backed
Python vulnerability audit.

`just image` builds the repository-named `musicbrainz-graph-enricher:local` image, verifies the
installed service import, and checks its numeric non-root runtime identity. `just
release-dry-run` produces local checksums, an SBOM, third-party notices, and provenance without
creating a tag, upload, release, or repository setting.

The CI caller pins the organization Automation workflows to an immutable commit.
**Public-library cutover: complete.** The `python-libraries` dependency is public, pinned to an
immutable commit, and fetched without private-package credentials. Ordinary and
Dependabot-authored pull requests run the same required job graph. The CI caller retains the
supported explicit `CODECOV_TOKEN`, and the release caller retains the supported
`prepare-image-command`; neither caller inherits secrets. Releases remain tag-only, and no
Renovate workflow is active.

The first-party package is MIT licensed. Dependency rights and vulnerabilities are checked from
the Python 3.14 lock before release approval. Publication additionally requires a sanitized
reachable history, a reviewed green commit, successful hosted CI, and explicit operator
approval. Visibility, tags, packages, images, and releases remain separate gates.
