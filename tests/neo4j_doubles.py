"""Interface-faithful Neo4j doubles shared by the test suite."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

from common import AsyncResilientNeo4jDriver
from neo4j import AsyncManagedTransaction, AsyncResult, AsyncSession, ResultSummary, SummaryCounters


DEFAULT_RECORD = {"matched_id": 12345}


def neo4j_result(
    *,
    record: Any = DEFAULT_RECORD,
    relationships_created: int = 1,
    contains_updates: bool = True,
) -> AsyncMock:
    """Return a result whose async methods and summary match the driver API."""
    counters = MagicMock(spec_set=SummaryCounters)
    counters.relationships_created = relationships_created
    counters.contains_updates = contains_updates

    # ``counters`` is populated per instance by the real ResultSummary, so use
    # a regular spec (rather than spec_set) while retaining API introspection.
    summary = MagicMock(spec=ResultSummary)
    summary.counters = counters

    result = AsyncMock(spec_set=AsyncResult)
    result.single.return_value = record
    result.consume.return_value = summary
    return result


def neo4j_transaction(
    *,
    record: Any = DEFAULT_RECORD,
    relationships_created: int = 1,
    contains_updates: bool = True,
) -> AsyncMock:
    """Return a transaction with an awaitable ``run`` yielding a strict result."""
    transaction = AsyncMock(spec_set=AsyncManagedTransaction)
    transaction.run.return_value = neo4j_result(
        record=record,
        relationships_created=relationships_created,
        contains_updates=contains_updates,
    )
    return transaction


def execute_write_with(transaction: AsyncMock) -> Any:
    """Return an awaitable execute_write implementation invoking its callback."""

    async def execute(transaction_function: Any, *args: Any, **kwargs: Any) -> Any:
        return await transaction_function(transaction, *args, **kwargs)

    return execute


def neo4j_session(*, transaction: AsyncMock | None = None, health_record: Any = None) -> AsyncMock:
    """Return the async context manager produced by ``driver.session()``."""
    session = AsyncMock(spec_set=AsyncSession)
    session.__aenter__.return_value = session
    session.__aexit__.return_value = False
    session.run.return_value = neo4j_result(record={"test": 1} if health_record is None else health_record)
    if transaction is not None:
        session.execute_write.side_effect = execute_write_with(transaction)
    return session


def neo4j_driver(*, session: AsyncMock | None = None) -> MagicMock:
    """Return the service's resilient driver with a synchronous session factory."""
    driver = MagicMock(spec_set=AsyncResilientNeo4jDriver)
    driver.session.return_value = session or neo4j_session()
    return driver
