# Internal package compatibility

`brainzgraphinator` is the historical Python package name used by the
`musicbrainz-graph-enricher` console command. It is retained to avoid breaking imports and the
v1 catalog-event consumer identifier used in durable RabbitMQ queue names.

The public service identity is `musicbrainz-graph-enricher`. Start with the repository
[README](../README.md) and [documentation index](../docs/README.md) for behavior,
configuration, and operations. New public examples should use the repository name; use
`brainzgraphinator` only when referring to a Python import or the documented AMQP wire token.
