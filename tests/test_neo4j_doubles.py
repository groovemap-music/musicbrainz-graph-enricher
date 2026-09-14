"""Regression tests for the Neo4j fixture contract."""

import inspect

import pytest

from tests.neo4j_doubles import neo4j_driver, neo4j_session, neo4j_transaction


@pytest.mark.asyncio
async def test_driver_session_matches_the_resilient_context_manager_contract() -> None:
    session = neo4j_session()
    driver = neo4j_driver(session=session)

    context = driver.session(database="neo4j")

    assert not inspect.isawaitable(context)
    async with context as entered:
        assert entered is session
    driver.session.assert_called_once_with(database="neo4j")


@pytest.mark.asyncio
async def test_transaction_run_and_result_methods_are_awaitable() -> None:
    transaction = neo4j_transaction(record={"matched_id": 42})

    run_call = transaction.run("RETURN 42 AS matched_id")
    assert inspect.isawaitable(run_call)
    result = await run_call
    assert await result.single() == {"matched_id": 42}
    assert (await result.consume()).counters.relationships_created == 1


@pytest.mark.asyncio
async def test_execute_write_invokes_the_transaction_callback() -> None:
    transaction = neo4j_transaction(record={"matched_id": 7})
    session = neo4j_session(transaction=transaction)

    async def write(tx):
        return await (await tx.run("RETURN 7 AS matched_id")).single()

    assert await session.execute_write(write) == {"matched_id": 7}
    session.execute_write.assert_awaited_once_with(write)


def test_boundary_doubles_reject_unknown_driver_attributes() -> None:
    driver = neo4j_driver()

    with pytest.raises(AttributeError):
        driver.sesion(database="neo4j")


def test_boundary_doubles_reject_unknown_transaction_and_result_attributes() -> None:
    transaction = neo4j_transaction()

    with pytest.raises(AttributeError):
        transaction.execute("RETURN 1")
    with pytest.raises(AttributeError):
        transaction.run.return_value.singel()
